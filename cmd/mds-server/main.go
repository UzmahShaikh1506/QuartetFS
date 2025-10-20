package main

import (
	"context"
	"log"
	"net"
	"time"

	pb "github.com/UzmahShaikh1506/QuartetFS/api"
	"github.com/google/uuid"
	"google.golang.org/grpc"
)

const port = ":50051" // Port for the MDS to listen on

// A simple in-memory store for file metadata.
// In a real system, this would be a database.
var fileMetadata = make(map[string][]string)

type server struct {
	pb.UnimplementedMetadataServiceServer
}

// InitiateUpload tells the client where to send its chunks.
func (s *server) InitiateUpload(ctx context.Context, in *pb.InitiateUploadRequest) (*pb.InitiateUploadResponse, error) {
	log.Printf("Received InitiateUpload for file: %s", in.GetFilename())

	// Generate a unique ID for the file.
	fileID := uuid.New().String()
	
	// For now, we only have one Storage Node. Tell the client to send all chunks there.
	// The key in this map is just a placeholder for now.
	locations := map[string]string{
		"default_sn": "localhost:50052",
	}
	
	return &pb.InitiateUploadResponse{FileId: fileID, ChunkUploadLocations: locations}, nil
}

// FinalizeUpload confirms the file is fully uploaded.
func (s *server) FinalizeUpload(ctx context.Context, in *pb.FinalizeUploadRequest) (*pb.FinalizeUploadResponse, error) {
	// Store the list of chunk hashes associated with the file ID.
	fileMetadata[in.GetFileId()] = in.GetChunkHashes()

	log.Printf("Successfully finalized upload for file ID: %s with %d chunks", in.GetFileId(), len(in.GetChunkHashes()))
	log.Printf("Current metadata state: %+v", fileMetadata)
	
	// Artificially add a small delay to simulate database write
	time.Sleep(100 * time.Millisecond)

	return &pb.FinalizeUploadResponse{Success: true}, nil
}


func main() {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterMetadataServiceServer(s, &server{})
	log.Printf("Metadata server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}