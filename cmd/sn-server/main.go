package main

import (
	"context"
	
	"log"
	"net"
	"os"
	"path/filepath"

	pb "github.com/UzmahShaikh1506/QuartetFS/api"
	"google.golang.org/grpc"
)

const storageDir = "tmp/storage" // Directory to save chunks
const port = ":50052"             // Port for the SN to listen on

type server struct {
	pb.UnimplementedStorageNodeServiceServer
}

// StoreChunk implements the gRPC service. It receives a chunk and saves it.
func (s *server) StoreChunk(ctx context.Context, in *pb.StoreChunkRequest) (*pb.StoreChunkResponse, error) {
	filePath := filepath.Join(storageDir, in.GetChunkHash())
	err := os.WriteFile(filePath, in.GetData(), 0644)
	if err != nil {
		log.Printf("Failed to write chunk %s: %v", in.GetChunkHash(), err)
		return &pb.StoreChunkResponse{Success: false, Message: err.Error()}, err
	}

	log.Printf("Successfully stored chunk %s", in.GetChunkHash())
	return &pb.StoreChunkResponse{Success: true, Message: "Chunk stored"}, nil
}

func main() {
	// Create the storage directory if it doesn't exist.
	if err := os.MkdirAll(storageDir, os.ModePerm); err != nil {
		log.Fatalf("failed to create storage dir: %v", err)
	}

	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterStorageNodeServiceServer(s, &server{})
	log.Printf("Storage Node server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}