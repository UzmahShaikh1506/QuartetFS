package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"path/filepath"

	pb "github.com/UzmahShaikh1506/QuartetFS/api"
	"google.golang.org/grpc"
)

const maxMsgSize = 5 * 1024 * 1024

// server struct now holds the storage directory path for this specific node.
type server struct {
	pb.UnimplementedStorageNodeServiceServer
	storageDir string
}

// StoreChunk receives a chunk and saves it to this node's storage directory.
func (s *server) StoreChunk(ctx context.Context, in *pb.StoreChunkRequest) (*pb.StoreChunkResponse, error) {
	filePath := filepath.Join(s.storageDir, in.GetChunkHash())
	err := os.WriteFile(filePath, in.GetData(), 0644)
	if err != nil {
		log.Printf("Failed to write chunk %s: %v", in.GetChunkHash(), err)
		return &pb.StoreChunkResponse{Success: false, Message: err.Error()}, err
	}
	log.Printf("Successfully stored chunk %s", in.GetChunkHash())
	return &pb.StoreChunkResponse{Success: true, Message: "Chunk stored"}, nil
}

// GetChunk reads a chunk from this node's storage directory and returns it.
func (s *server) GetChunk(ctx context.Context, in *pb.GetChunkRequest) (*pb.GetChunkResponse, error) {
	filePath := filepath.Join(s.storageDir, in.GetChunkHash())
	data, err := os.ReadFile(filePath)
	if err != nil {
		log.Printf("Failed to read chunk %s: %v", in.GetChunkHash(), err)
		return nil, err
	}
	log.Printf("Serving chunk %s", in.GetChunkHash())
	return &pb.GetChunkResponse{Data: data}, nil
}

func main() {
	// Command-line flags allow us to run multiple SNs with different configs.
	port := flag.String("port", ":50052", "The port for the server to listen on")
	storageDir := flag.String("dir", "tmp/storage/sn1", "The directory to store chunks")
	flag.Parse() // Parse the command-line arguments

	// Use the parsed flags to create the unique storage directory for this node.
	if err := os.MkdirAll(*storageDir, os.ModePerm); err != nil {
		log.Fatalf("failed to create storage dir %s: %v", *storageDir, err)
	}

	lis, err := net.Listen("tcp", *port)
	if err != nil {
		log.Fatalf("failed to listen on port %s: %v", *port, err)
	}

	s := grpc.NewServer(grpc.MaxRecvMsgSize(maxMsgSize))

	// Pass the unique storage directory to the server struct instance.
	pb.RegisterStorageNodeServiceServer(s, &server{storageDir: *storageDir})

	log.Printf("Storage Node server starting. Listening at %v. Storing data in %s.", lis.Addr(), *storageDir)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}