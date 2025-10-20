package fileops

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log"
	"os"
)

// ChunkSize is the fixed size of each data chunk in bytes (e.g., 4MB).
const ChunkSize = 4 * 1024 * 1024

// Chunk represents a piece of a file.
type Chunk struct {
	Index int    // The position of the chunk in the file (0, 1, 2, ...)
	Data  []byte // The raw byte data of the chunk
	Hash  string // The SHA-256 hash of the Data, used as a unique ID
}

// ChunkFile reads a file from the given path and splits it into hashed Chunks.
func ChunkFile(filePath string) ([]Chunk, error) {
	// Open the file for reading.
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err // Return an error if the file can't be opened.
	}
	defer file.Close() // Ensure the file is closed when the function exits.

	var chunks []Chunk
	buffer := make([]byte, ChunkSize)
	chunkIndex := 0

	for {
		// Read data from the file into our buffer.
		bytesRead, err := file.Read(buffer)
		if err != nil {
			if err == io.EOF {
				break // End of file, we're done.
			}
			return nil, err // Another type of read error occurred.
		}

		// We only care about the actual bytes read, not the full buffer size.
		data := buffer[:bytesRead]

		// Calculate the SHA-256 hash of the chunk's data.
		hash := sha256.Sum256(data)
		hashString := hex.EncodeToString(hash[:])

		// Create a new Chunk and add it to our slice.
		chunks = append(chunks, Chunk{
			Index: chunkIndex,
			Data:  data, // Important: Use a copy of the data
			Hash:  hashString,
		})

		chunkIndex++
	}

	log.Printf("Finished chunking file '%s' into %d chunks.", filePath, len(chunks))
	return chunks, nil
}