package nwfslib

// common functions needed by both the server and the client
import (
	"fmt"
	"log"
	"crypto/sha256"
	"math/rand"
	"encoding/json"
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

func write_shards(filename string, contents []byte, backends []string) (*Metadata, error) {

	meta := new(Metadata)
	for _, sh := range divide_into_shards(contents) {

		c := Nwfs_rpc_client{Backends: get_backends(backends, NUM_BACK)}

		err := c.Connect()
		if err != nil {
			return nil, err
		}
		defer c.Close()

		hash := ""
		_, err = c.WriteShard(sh, &hash)
		if err != nil {
			return nil, err
		}

		meta.Shards = append(meta.Shards, hash)
		meta.Backs = append(meta.Backs, c.Backends)
	}

	return meta, nil
}

func get_backends(backs []string, num int) []string {

	back_num := len(backs)
	chosen_backs := make([]string, num)

	for i:=0; i<num; i++ {
		choose := rand.Intn(back_num-i)
		chosen_backs[i] = backs[i+choose]
		backs[choose] = backs[i]
	}

	return chosen_backs
}

func Read_op(meta_bytes []byte, op []string) (string, error) {

	var meta Metadata
	err := json.Unmarshal(meta_bytes, &meta)
	if err != nil {
		log.Println("json.Unmarshal failed: ", err)
		return "", err
	}

	output := ""
	for i, backs := range meta.Backs {
		shard := meta.Shards[i]
		c := Nwfs_rpc_client{Backends: backs}

		err := c.Connect()
		if err != nil {
			return "", err
		}
		defer c.Close()

		op_output := ""
		err = c.Read_op(&ReadArgs{Hash: shard, Op: op}, &op_output)
		if err != nil {
			return "", err
		}

		output += op_output
	}

	return output, nil
}
