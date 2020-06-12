package nwfslib

// common functions needed by both the server and the client
import (
	"fmt"
	"runtime"
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

func Divide_into_shards(contents []byte) [][]byte {
	shards := make([][]byte, 0)
	for len(contents)>0 {
		var ll int
		if len(contents) < SHARD_SIZE {
			ll = len(contents)
		} else {
			for ll = SHARD_SIZE; ll < len(contents); ll++ {
				if contents[ll] == '\n' {
					break;
				}
			}
		}

		shards = append(shards, contents[:ll])
		contents = contents[ll:]
	}

	if VERBOSE_LOGS {
		log.Println(__FUNC__(), len(shards), "shards created")
	}

	return shards
}

func Write_shards(filename string, contents []byte, backends []string) (*Metadata, error) {
	
	if ENTRY_ARG_LOGS {
		log.Println(__FUNC__(), filename, backends)
	}

	shard_arr := Divide_into_shards(contents)
	type ws_data struct {
		hash string
		backends []string
		err error
	}
	ch_arr := make([]chan ws_data, len(shard_arr))

	for i, sh := range shard_arr {
		ch_arr[i] = make(chan ws_data, 1);

		go func(ch chan ws_data, sh []byte) {
			c := Nwfs_rpc_client{Backends: get_backends(backends, NUM_BACK)}

			err := c.Connect()
			if err != nil {
				ch<-ws_data{"", nil, err}
				return
				//return nil, err
			}
			defer c.Close()

			hash := ""
			_, err = c.WriteShard(sh, &hash)
			if err != nil {
				ch<-ws_data{"", nil, err}
				return
			}

			ch <- ws_data{hash, c.Backends, nil}
		}(ch_arr[i], sh)
	}

	meta := new(Metadata)
	for i := range shard_arr {
		ws := <-ch_arr[i]
		if ws.err != nil {
			return nil, ws.err
		}
		meta.Shards = append(meta.Shards, ws.hash)
		meta.Backs = append(meta.Backs, ws.backends)
	}

	if VERBOSE_LOGS {
		log.Println(__FUNC__(), filename, "metadata:", meta)
	}

	return meta, nil
}

func Write_op_shards(meta_bytes []byte, op []string) (*Metadata, error) {
	if ENTRY_ARG_LOGS {
		log.Println(__FUNC__(), op)
	}

	var meta Metadata
	err := json.Unmarshal(meta_bytes, &meta)
	if err != nil {
		log.Println("json.Unmarshal failed: ", err)
		return nil,err
	}
	type wo_data struct {
		hash string
		err error
	}

	var new_meta Metadata
	new_meta.Shards = make([]string, len(meta.Backs))
	new_meta.Backs = make([][]string, len(meta.Backs))
	ch_arr := make([]chan wo_data, len(meta.Backs))

	for i, backs := range meta.Backs {
		ch_arr[i] = make(chan wo_data, 1);
		shard := meta.Shards[i]
		go func(ch chan wo_data, shard string, backs []string) {
			c := Nwfs_rpc_client{Backends: backs}

			err := c.Connect()
			if err != nil {
				ch <- wo_data{"", err}
				return
			}
			defer c.Close()

			hash := ""
			err = c.Write_op_shards(&ReadArgs{Hash: shard, Op: op}, &hash)
			if err != nil {
				ch <- wo_data{"", err}
				return
			}

			ch <- wo_data{hash, nil}
		}(ch_arr[i], shard, backs)
	}

	for i, b := range meta.Backs {
		wo := <-ch_arr[i]
		if wo.err != nil {
			return nil, wo.err
		}
		new_meta.Shards[i] = wo.hash
		new_meta.Backs[i] = make([]string, 0)
		new_meta.Backs[i] = append(new_meta.Backs[i], b...)
	}

	return &new_meta, nil
}

func get_backends(_backs []string, num int) []string {

	backs := make([]string, 0)
	backs = append(backs, _backs...)

	if ENTRY_ARG_LOGS {
		log.Println(__FUNC__(), backs, num)
	}

	back_num := len(backs)
	chosen_backs := make([]string, num)

	for i:=0; i<num; i++ {
		choose := rand.Intn(10000)%(back_num-i)
		chosen_backs[i] = backs[i+choose]
		backs[choose] = backs[i]
	}

	if ENTRY_ARG_LOGS {
		log.Println(__FUNC__(), "exit:", chosen_backs)
	}
	return chosen_backs
}

func Read_op(meta_bytes []byte, op []string) (string, error) {

	if ENTRY_ARG_LOGS {
		log.Println(__FUNC__(), op)
	}

	var meta Metadata
	err := json.Unmarshal(meta_bytes, &meta)
	if err != nil {
		log.Println("json.Unmarshal failed: ", err)
		return "", err
	}
	type ro_data struct {
		s string
		err error
	}

	ch_arr := make([]chan ro_data, len(meta.Backs))

	output := ""
	for i, backs := range meta.Backs {
		ch_arr[i] = make(chan ro_data, 1);
		shard := meta.Shards[i]
		go func(ch chan ro_data, shard string, backs []string) {
			c := Nwfs_rpc_client{Backends: backs}

			err := c.Connect()
			if err != nil {
				ch <- ro_data{"", err}
				return
			}
			defer c.Close()

			op_output := ""
			err = c.Read_op(&ReadArgs{Hash: shard, Op: op}, &op_output)
			if err != nil {
				ch <- ro_data{"", err}
				return
			}

			ch <- ro_data{op_output, nil}
		}(ch_arr[i], shard, backs)
	}

	for i, _ := range meta.Backs {
		ro := <-ch_arr[i]
		if ro.err != nil {
			return "", ro.err
		}
		output += ro.s
	}

	if VERBOSE_LOGS {
		if len(output) < 1000 {
			log.Println(__FUNC__(), "output:", output)
		} else {
			log.Println(__FUNC__(), "truncated output:", output[:200])
		}
	}

	return output, nil
}

/* get the function name
 * from  https://stackoverflow.com/questions/25927660/how-to-get-the-current-function-name/46289376#46289376
 */
func __FUNC__() string {

	pc := make([]uintptr, 15)
	n := runtime.Callers(2, pc)
	frames := runtime.CallersFrames(pc[:n])
	frame, _ := frames.Next()
	return frame.Function + "():"
}
