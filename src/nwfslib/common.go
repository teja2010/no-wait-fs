package nwfslib

// common functions needed by both the server and the client
import (
	"fmt"
	"crypto/sha256"
)

type ReadArgs struct {
	Hash string
	Op []string // all gaps will be filled with the file path
		    // e.g. ["grep LOG", "| grep error"]
		    //  runs: grep LOG <file_name> | grep error
}

func Shard_hash(input []byte) string {

	sha := sha256.New()
	sha.Write(input)
	hash := sha.Sum(nil)
	out := fmt.Sprintf("%x", hash)

	return string(out)
}

func divide_into_shards(contents []byte) [][]byte {
	shards := make([][]byte, 0)
	for len(contents)>0 {
		ll := SHARD_SIZE
		if len(contents) < SHARD_SIZE {
			ll = len(contents)
		}
		shards = append(shards, contents[:ll])
		contents = contents[ll:]
	}

	return shards
}
