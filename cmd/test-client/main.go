package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	pb "github.com/UzmahShaikh1506/QuartetFS/api"
	"github.com/UzmahShaikh1506/QuartetFS/internal/fileops"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const maxMsgSize = 5 * 1024 * 1024
const fileIDTracker = "tmp/last_upload.txt"

func main() {
	// Command-line flags to choose which action to perform.
	uploadCmd := flag.String("upload", "", "Path of the file to upload")
	downloadCmd := flag.String("download", "", "Path to save the downloaded file")
	flag.Parse()

	if *uploadCmd != "" {
		runUpload(*uploadCmd)
	} else if *downloadCmd != "" {
		runDownload(*downloadCmd)
	} else {
		log.Println("No action specified. Use -upload <filepath> or -download <filepath>")
	}
}

// runUpload orchestrates the entire file upload process.
func runUpload(filePath string) {
	log.Println("--- STARTING UPLOAD ---")

	// Step 1: Chunk the local file and collect hashes.
	log.Println("Step 1: Chunking the file...")
	chunks, err := fileops.ChunkFile(filePath)
	if err != nil {
		log.Fatalf("Failed to chunk file: %v", err)
	}
	fileInfo, _ := os.Stat(filePath)
	var chunkHashes []string
	for _, chunk := range chunks {
		chunkHashes = append(chunkHashes, chunk.Hash)
	}

	// Step 2: Contact MDS for a replication plan.
	log.Println("Step 2: Contacting Metadata Service for replication plan...")
	mdsConn, err := grpc.Dial("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Did not connect to MDS: %v", err)
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
		log.Fatalf("Could not initiate upload: %v", err)
	}
	log.Printf("MDS responded. File ID: %s. Replicating to nodes: %v", initRes.FileId, initRes.UploadPlan.NodeAddresses)

	// Step 3: Upload each chunk to all its replicas in parallel.
	log.Println("Step 3: Uploading chunks to all replicas in parallel...")
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

	// Step 4: Finalize the upload with the MDS.
	log.Println("Step 4: Finalizing upload with Metadata Service...")
	_, err = mdsClient.FinalizeUpload(ctx, &pb.FinalizeUploadRequest{
		FileId:      initRes.FileId,
		ChunkHashes: chunkHashes,
	})
	if err != nil {
		log.Fatalf("Could not finalize upload: %v", err)
	}

	// Step 5: Save the file ID for easy downloading.
	os.WriteFile(fileIDTracker, []byte(initRes.FileId), 0644)
	log.Printf("Saved file ID %s for next download.", initRes.FileId)
	log.Println("--- UPLOAD COMPLETE! ---")
}

// runDownload orchestrates the entire file download and reassembly process.
func runDownload(savePath string) {
	log.Println("--- STARTING DOWNLOAD ---")

	// Step 1: Get the ID of the last uploaded file.
	fileIDBytes, err := os.ReadFile(fileIDTracker)
	if err != nil {
		log.Fatalf("Could not read last file ID. Did you upload a file first? Error: %v", err)
	}
	fileID := string(fileIDBytes)
	log.Printf("Step 1: Attempting to download file with ID: %s", fileID)

	// Step 2: Contact MDS for chunk locations.
	log.Println("Step 2: Contacting Metadata Service for chunk locations...")
	mdsConn, err := grpc.Dial("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Did not connect to MDS: %v", err)
	}
	defer mdsConn.Close()
	mdsClient := pb.NewMetadataServiceClient(mdsConn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	locRes, err := mdsClient.GetDownloadLocations(ctx, &pb.DownloadLocationsRequest{FileId: fileID})
	if err != nil {
		log.Fatalf("Could not get download locations: %v", err)
	}
	log.Printf("Received download plan for %d chunks.", len(locRes.ChunkLocations))

	// Step 3: Download all chunks in parallel, with retry logic.
	log.Println("Step 3: Downloading all chunks in parallel...")
	chunkData := make(map[string][]byte) // Map to store chunk data as it arrives.
	var mu sync.Mutex                    // Mutex to safely write to the map from multiple goroutines.
	var wg sync.WaitGroup

	for _, chunkInfo := range locRes.ChunkLocations {
		wg.Add(1)
		go func(info *pb.ChunkDownloadInfo) {
			defer wg.Done()

			// --- NEW RESILIENT DOWNLOAD LOGIC ---
			var downloadedData []byte
			var lastErr error
			// Iterate through all available nodes for this chunk.
			for _, addr := range info.NodeAddresses {
				conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsgSize)))
				if err != nil {
					lastErr = fmt.Errorf("could not connect to SN at %s: %v", addr, err)
					log.Printf("WARNING: %v", lastErr)
					continue // Try the next node.
				}
				defer conn.Close()

				snClient := pb.NewStorageNodeServiceClient(conn)
				res, err := snClient.GetChunk(ctx, &pb.GetChunkRequest{ChunkHash: info.ChunkHash})
				if err != nil {
					lastErr = fmt.Errorf("could not get chunk %s from %s: %v", info.ChunkHash, addr, err)
					log.Printf("WARNING: %v", lastErr)
					continue // Try the next node.
				}

				// If we get here, the download was successful!
				downloadedData = res.Data
				log.Printf("Successfully downloaded chunk %s from node %s", info.ChunkHash, addr)
				break // Exit the loop since we have the data.
			}

			if downloadedData != nil {
				mu.Lock()
				chunkData[info.ChunkHash] = downloadedData
				mu.Unlock()
			} else {
				log.Printf("FATAL: Could not download chunk %s from any of its replicas. Last error: %v", info.ChunkHash, lastErr)
			}
		}(chunkInfo)
	}
	wg.Wait()
	log.Println("All available chunks downloaded.")

	// Step 4: Reassemble the file in the correct order.
	log.Println("Step 4: Reassembling file...")
	destFile, err := os.Create(savePath)
	if err != nil {
		log.Fatalf("Could not create destination file: %v", err)
	}
	defer destFile.Close()

	// Check if we have all the chunks before writing.
	if len(chunkData) != len(locRes.ChunkLocations) {
		log.Fatalf("Download failed: missing %d chunks.", len(locRes.ChunkLocations)-len(chunkData))
	}

	// Iterate through the original chunk list from the MDS to ensure correct order.
	for _, chunkInfo := range locRes.ChunkLocations {
		hash := chunkInfo.ChunkHash
		data, ok := chunkData[hash]
		if !ok {
			log.Fatalf("FATAL: Missing chunk data for hash %s during reassembly. This should not happen.", hash)
		}
		if _, err := destFile.Write(data); err != nil {
			log.Fatalf("Could not write chunk %s to file: %v", hash, err)
		}
	}

	log.Printf("--- DOWNLOAD COMPLETE! File saved to %s ---", savePath)
}

// uploadChunkToReplicas is a helper function to send a single chunk to all its replicas.
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
