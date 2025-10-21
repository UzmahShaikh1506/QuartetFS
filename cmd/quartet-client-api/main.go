package main

import (
	"encoding/json"
	"log"
	"net/http"

	// We can reuse all the logic we already wrote!
	pb "github.com/UzmahShaikh1506/Quartetfs/api"
	"github.com/UzmahShaikh1506/Quartetfs/internal/fileops"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"context"
	"os"
	"sync"
	"time"
)

// This file combines the logic of our test-client with an HTTP server.

const maxMsgSize = 5 * 1024 * 1024

// UploadRequest represents the JSON body we expect from the UI.
type UploadRequest struct {
	FilePath string `json:"filePath"`
}

func main() {
	// Define an HTTP endpoint that the UI will call.
	http.HandleFunc("/upload", handleUpload)

	log.Println("API Bridge server starting on http://localhost:8080")
	log.Println("Waiting for UI to send upload requests...")
	// Start the HTTP server. This will block forever, waiting for requests.
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("Failed to start API Bridge server: %v", err)
	}
}

// handleUpload is called every time the UI sends a request to the /upload endpoint.
func handleUpload(w http.ResponseWriter, r *http.Request) {
	// Enable CORS to allow the React app (on port 3000) to talk to this server (on port 8080).
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

	// Decode the JSON request from the UI.
	var req UploadRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	log.Printf("Received upload request from UI for file: %s", req.FilePath)

	// Run the entire upload process. This is the same logic from our test-client.
	// We run it in a goroutine so the UI gets an immediate response.
	go func() {
		err := runUpload(req.FilePath)
		if err != nil {
			// In a real app, you'd signal this error back to the UI (e.g., via WebSockets).
			log.Printf("ERROR during background upload: %v", err)
		}
	}()

	// Respond to the UI immediately.
	json.NewEncoder(w).Encode(map[string]string{"message": "Upload started successfully!"})
}

// runUpload contains the exact same logic as our previous test client.
func runUpload(filePath string) error {
	log.Println("--- STARTING UPLOAD ---")

	log.Println("Step 1: Chunking the file...")
	chunks, err := fileops.ChunkFile(filePath)
	if err != nil {
		return err
	}
	fileInfo, _ := os.Stat(filePath)
	var chunkHashes []string
	for _, chunk := range chunks {
		chunkHashes = append(chunkHashes, chunk.Hash)
	}

	log.Println("Step 2: Contacting Metadata Service...")
	mdsConn, err := grpc.Dial("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer mdsConn.Close()
	mdsClient := pb.NewMetadataServiceClient(mdsConn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	initRes, err := mdsClient.InitiateUpload(ctx, &pb.InitiateUploadRequest{
		Filename:      filePath,
		FilesizeBytes: fileInfo.Size(),
		ChunkHashes:   chunkHashes,
	})
	if err != nil {
		return err
	}
	log.Printf("MDS responded. File ID: %s. Replicating to nodes: %v", initRes.FileId, initRes.UploadPlan.NodeAddresses)

	log.Println("Step 3: Uploading chunks...")
	var wg sync.WaitGroup
	for _, chunk := range chunks {
		wg.Add(1)
		go func(c fileops.Chunk) {
			defer wg.Done()
			uploadChunkToReplicas(c, initRes.UploadPlan.NodeAddresses)
		}(chunk)
	}
	wg.Wait()
	log.Println("All chunks replicated successfully!")

	log.Println("Step 4: Finalizing upload...")
	_, err = mdsClient.FinalizeUpload(ctx, &pb.FinalizeUploadRequest{
		FileId:      initRes.FileId,
		ChunkHashes: chunkHashes,
	})
	if err != nil {
		return err
	}

	log.Println("--- UPLOAD COMPLETE! ---")
	return nil
}

// uploadChunkToReplicas is the same helper function from our test client.
func uploadChunkToReplicas(chunk fileops.Chunk, addresses []string) {
	for _, addr := range addresses {
		conn, err := grpc.Dial(addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(maxMsgSize)),
		)
		if err != nil {
			log.Printf("ERROR: Could not connect to SN at %s to send chunk %s: %v", addr, chunk.Hash, err)
			continue
		}
		defer conn.Close()

		snClient := pb.NewStorageNodeServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()

		_, err = snClient.StoreChunk(ctx, &pb.StoreChunkRequest{
			ChunkHash: chunk.Hash,
			Data:      chunk.Data,
		})
		if err != nil {
			log.Printf("ERROR: Failed to store chunk %s on node %s: %v", chunk.Hash, addr, err)
		} else {
			log.Printf("Successfully stored chunk %s on node %s", chunk.Hash, addr)
		}
	}
}