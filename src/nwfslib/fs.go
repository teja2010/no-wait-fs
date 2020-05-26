package nwfslib

import (
	//"errors"
	"log"
	"time"
	"math/rand"
	go_zk "github.com/samuel/go-zookeeper/zk"
	"zk_rculib"
	"encoding/json"
)

const (
	NUM_BACK = 3
	SHARD_SIZE = 1024
)

type fs_client struct {
	zk_servers []string
	backends   []string
	zk_conn    *go_zk.Conn
	zk_rcu     zk_rculib.Zk_RCU_res
}

type Metadata struct {
	Version int
	Shards []string
	Backs [][]string
}

func Open(filepath string, zk_servers, backends []string) (Fs_handle, error) {

	var err error
	f := new(fs_client)
	f.zk_servers = append(f.zk_servers, zk_servers...)
	f.backends = append(f.backends, backends...)
	f.zk_conn, _, err = go_zk.Connect(zk_servers, 3*time.Second)
	if err != nil {
		return nil, err
	}
	f.zk_rcu, err = zk_rculib.Create_RCU_resource(filepath, f.zk_conn)
	if err != nil {
		return nil, err
	}

	return f, nil
}

type Fs_handle interface {
	Read_op(filename string, op []string) (string, error)
	Write(filename string, contents []byte) (*Metadata, error)
	Write_meta(filename string, meta *Metadata) error
	Read_lock(filename string) error
	Read_unlock(filename string) error
	//Close()
}


func (f *fs_client) Read_op(filename string, op []string) (string, error) {
	meta_bytes, err := f.zk_rcu.Dereference()
	if err != nil {
		return "", err
	}

	var meta Metadata
	err = json.Unmarshal(meta_bytes, &meta)
	if err != nil {
		log.Println("json.Unmarshal failed: ", err)
		return "", err
	}

	output := ""
	for i, backs := range meta.Backs {
		shard := meta.Shards[i]
		c := nwfs_rpc_client{Backends: backs}

		err := c.Connect()
		if err != nil {
			return "", err
		}
		defer c.Close()

		op_output := ""
		err = c.Read_op(&ReadArgs{hash: shard, op: op}, &op_output)
		if err != nil {
			return "", err
		}
		
		output += op_output
	}

	return output, nil
}

func (f *fs_client) Write(filename string, contents []byte ) (*Metadata, error) {

	meta := new(Metadata)
	for _, sh := range divide_into_shards(contents) {

		c := nwfs_rpc_client{Backends: f.get_backends(NUM_BACK)}

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

func (f *fs_client) Write_meta(filename string, meta *Metadata) error {

	meta_bytes, err := json.Marshal(meta)
	if err != nil {
		log.Println("json.Marshal failed: ", err)
		return err
	}

	return f.zk_rcu.Assign(meta.Version, meta_bytes)
}

func (f *fs_client) Read_lock(filename string) error {
	return f.zk_rcu.Read_lock()
}

func (f *fs_client) Read_unlock(filename string) error {
	return f.zk_rcu.Read_unlock()
}

// helpers
// get a few backends to init
func (f *fs_client) get_backends(num int) []string {
	backs := make([]string,0)
	backs = append(backs, f.backends...)

	back_num := len(backs)
	chosen_backs := make([]string, num)

	for i:=0; i<num; i++ {
		choose := rand.Intn(back_num-i)
		chosen_backs[i] = backs[i+choose]
		backs[choose] = backs[i]
	}

	return chosen_backs
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

