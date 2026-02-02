// Package jobid provides generation of job IDs.
//
// Job ID Format:
//   - version: 1 byte (format version for future changes)
//   - timestamp: 6 bytes (milliseconds since epoch, 48-bit)
//   - app_id_hash: 4 bytes (first 4 bytes of SHA256(app_id))
//   - job_type_hash: 4 bytes (first 4 bytes of SHA256(job_type))
//   - random: 4 bytes (random bytes for uniqueness)
//   - checksum: 2 bytes (CRC16 for integrity)
//
// Total: 21 bytes -> Base62 encoded -> ~29 characters
// Example: pjob_7Xj9K2mNpQrStUvWxYz3A5B8C
//
// Collision Probability:
// Collisions require matching timestamp (same ms), app_id, job_type, AND random bytes.
// With 4 bytes (32 bits) of randomness, using birthday paradox approximation (p ≈ n²/2m):
//
//	~9,300 IDs/ms/app/job_type: ~1% collision probability
//
// This means that to have even a 1% chance of collision, you would need to create
// over 9,000 jobs for the same app and job type within a single millisecond.
package jobid

import (
	"crypto/rand"
	"crypto/sha256"
	"hash/crc32"
	"time"
)

const (
	// Prefix for all job IDs.
	Prefix = "pjob_"

	// Current format version.
	Version = 1

	// Field sizes in bytes.
	versionSize     = 1
	timestampSize   = 6
	appHashSize     = 4
	jobTypeHashSize = 4
	randomSize      = 4
	checksumSize    = 2

	// Total payload size before encoding.
	payloadSize = versionSize + timestampSize + appHashSize + jobTypeHashSize + randomSize + checksumSize // 21 bytes
)

// Base62 alphabet for encoding.
const base62Alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

// Generate creates a new job ID for the given app_id and job_type.
func Generate(appID, jobType string) string {
	payload := make([]byte, payloadSize)

	// Version (1 byte)
	payload[0] = Version

	// Timestamp (6 bytes) - milliseconds since epoch
	now := time.Now().UnixMilli()
	payload[1] = byte(now >> 40)
	payload[2] = byte(now >> 32)
	payload[3] = byte(now >> 24)
	payload[4] = byte(now >> 16)
	payload[5] = byte(now >> 8)
	payload[6] = byte(now)

	// App ID hash (4 bytes)
	appHash := sha256.Sum256([]byte(appID))
	copy(payload[7:11], appHash[:4])

	// Job type hash (4 bytes)
	jobHash := sha256.Sum256([]byte(jobType))
	copy(payload[11:15], jobHash[:4])

	// Random (4 bytes)
	rand.Read(payload[15:19])

	// Checksum (2 bytes) - CRC16 of the first 19 bytes
	checksum := crc16(payload[:19])
	payload[19] = byte(checksum >> 8)
	payload[20] = byte(checksum)

	return Prefix + encodeBase62(payload)
}

// encodeBase62 encodes bytes to a base62 string.
func encodeBase62(data []byte) string {
	if len(data) == 0 {
		return ""
	}

	// Convert bytes to a big integer (big-endian)
	// We'll use a simple algorithm that works with arbitrary precision
	result := make([]byte, 0, len(data)*2)

	// Work with a copy of the data
	num := make([]byte, len(data))
	copy(num, data)

	for !isZero(num) {
		remainder := divideBy62(num)
		result = append(result, base62Alphabet[remainder])
	}

	// Reverse the result
	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}

	// Handle leading zeros in input
	for _, b := range data {
		if b != 0 {
			break
		}
		result = append([]byte{base62Alphabet[0]}, result...)
	}

	if len(result) == 0 {
		return string(base62Alphabet[0])
	}

	return string(result)
}

// isZero checks if all bytes are zero.
func isZero(data []byte) bool {
	for _, b := range data {
		if b != 0 {
			return false
		}
	}
	return true
}

// divideBy62 divides the big-endian number by 62, modifying it in place,
// and returns the remainder.
func divideBy62(num []byte) int {
	var remainder int
	for i := 0; i < len(num); i++ {
		val := remainder*256 + int(num[i])
		num[i] = byte(val / 62)
		remainder = val % 62
	}
	return remainder
}

// crc16 computes a simple CRC16 checksum.
func crc16(data []byte) uint16 {
	// Using CRC32 and taking the lower 16 bits for simplicity
	crc := crc32.ChecksumIEEE(data)
	return uint16(crc & 0xFFFF)
}
