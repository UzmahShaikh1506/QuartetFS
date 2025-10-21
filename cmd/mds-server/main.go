package main

import (
	"context"
	"log"
	"math/rand"
	"net"
	"time"

	pb "github.com/UzmahShaikh1506/QuartetFS/api"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const port = ":50051"
const replicationFactor = 3

// fileInfo struct holds the metadata for a single file.
type fileInfo struct {
	filename    string
	chunkHashes []string
}

// In-memory stores for our metadata. In a real system, this would be a database.
var fileMetadata = make(map[string]fileInfo)      // Maps fileID to fileInfo
var chunkLocations = make(map[string][]string) // Maps chunkHash to list of SN addresses

// A list of our available Storage Nodes. In a real system, this would be dynamic.
var availableStorageNodes = []string{
	"localhost:50052",
	"localhost:50053",
	"localhost:50054",
}

type server struct {
	pb.UnimplementedMetadataServiceServer
}

// InitiateUpload creates a replication plan for a new file upload.
func (s *server) InitiateUpload(ctx context.Context, in *pb.InitiateUploadRequest) (*pb.InitiateUploadResponse, error) {
	log.Printf("Received InitiateUpload for file: %s", in.GetFilename())
	fileID := uuid.New().String()

	// --- CORRECTED REPLICATION LOGIC ---
	// Create a single replication plan for all chunks of this file.
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(availableStorageNodes), func(i, j int) {
		availableStorageNodes[i], availableStorageNodes[j] = availableStorageNodes[j], availableStorageNodes[i]
	})

	var replicaAddresses []string
	// Ensure we don't try to select more replicas than available nodes.
	for i := 0; i < replicationFactor && i < len(availableStorageNodes); i++ {
		replicaAddresses = append(replicaAddresses, availableStorageNodes[i])
	}

	// For each chunk, associate it with the chosen replica nodes. This is important for downloads.
	for _, hash := range in.GetChunkHashes() {
		chunkLocations[hash] = replicaAddresses
	}

	uploadPlan := &pb.ChunkUploadInfo{
		NodeAddresses: replicaAddresses,
	}

	log.Printf("Generated upload plan for file %s. Replicas: %v", fileID, replicaAddresses)

	return &pb.InitiateUploadResponse{FileId: fileID, UploadPlan: uploadPlan}, nil
}

// FinalizeUpload marks the file as fully uploaded and stores its metadata.
func (s *server) FinalizeUpload(ctx context.Context, in *pb.FinalizeUploadRequest) (*pb.FinalizeUploadResponse, error) {
	// In a real system, you'd also get the original filename here.
	fileMetadata[in.GetFileId()] = fileInfo{
		chunkHashes: in.GetChunkHashes(),
	}

	log.Printf("Successfully finalized upload for file ID: %s with %d chunks", in.GetFileId(), len(in.GetChunkHashes()))
	return &pb.FinalizeUploadResponse{Success: true}, nil
}

// GetDownloadLocations looks up a file's chunks and tells the client where to find them.
func (s *server) GetDownloadLocations(ctx context.Context, in *pb.DownloadLocationsRequest) (*pb.DownloadLocationsResponse, error) {
	info, ok := fileMetadata[in.GetFileId()]
	if !ok {
		log.Printf("File ID not found: %s", in.GetFileId())
		return nil, status.Errorf(codes.NotFound, "file ID not found")
	}

	var locations []*pb.ChunkDownloadInfo
	for _, hash := range info.chunkHashes {
		addrs, found := chunkLocations[hash]
		if !found {
			// This would indicate a major data consistency issue in a real system.
			return nil, status.Errorf(codes.Internal, "metadata inconsistency: chunk %s locations not found", hash)
		}
		locations = append(locations, &pb.ChunkDownloadInfo{
			ChunkHash:     hash,
			NodeAddresses: addrs,
		})
	}

	log.Printf("Serving download locations for file ID: %s", in.GetFileId())
	return &pb.DownloadLocationsResponse{ChunkLocations: locations}, nil
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