package nwfslib

// common functions needed by both the server and the client
import (
	"fmt"
	"crypto/sha256"
)

func Shard_hash(input []byte) string {

	sha := sha256.New()
	sha.Write(input)
	hash := sha.Sum(nil)
	out := fmt.Sprintf("%x", hash)

	return string(out)
}
