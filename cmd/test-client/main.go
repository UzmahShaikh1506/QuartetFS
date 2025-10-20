package main

import (
	"fmt"
	"log"

	"github.com/UzmahShaikh1506/quartetfs/internal/fileops"
)

func main() {
	// The path to the test file you just created.
	testFilePath := "testfile.tmp"

	log.Println("Starting file chunking test...")

	chunks, err := fileops.ChunkFile(testFilePath)
	if err != nil {
		log.Fatalf("Error chunking file: %v", err)
	}

	// Print the results to verify it's working.
	for _, chunk := range chunks {
		// We print the data size to confirm the last chunk is smaller.
		fmt.Printf("Chunk %d -- Hash: %s -- Size: %d bytes\n", chunk.Index, chunk.Hash, len(chunk.Data))
	}

	log.Println("File chunking test completed successfully!")
}
