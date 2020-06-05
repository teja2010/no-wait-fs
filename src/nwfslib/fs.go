package nwfslib

import (
	"errors"
	"log"
	"time"
	"math/rand"
	go_zk "github.com/samuel/go-zookeeper/zk"
	"zk_rculib"
	"encoding/json"
)

const (
	NUM_BACK = 1 // Not handling multiple replicas
	SHARD_SIZE = 1024
)

type fs_client_rcu struct {
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

func Open(filepath string, zk_servers, backends []string, locking string) (Fs_handle, error) {

	switch locking {
	case "RCU":
		var err error
		f := new(fs_client_rcu)
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
	
	case "NoLock":
		var err error
		f := new(fs_client_nolock)
		f.zk_servers = append(f.zk_servers, zk_servers...)
		f.backends = append(f.backends, backends...)

		f.zk_conn, _, err = go_zk.Connect(zk_servers, 3*time.Second)
		if err != nil {
			return nil, err
		}

		f.nl, err = Create_no_lock_res(filepath, f.zk_conn)
		if err != nil {
			return nil, err
		}
		return f, nil
	
	case "SingleLock":
		var err error
		f := new(fs_client_singleLock)
		f.zk_servers = append(f.zk_servers, zk_servers...)
		f.backends = append(f.backends, backends...)
		f.zk_conn, _, err = go_zk.Connect(zk_servers, 3*time.Second)
		if err != nil {
			return nil, err
		}

		f.lock, err = Create_single_lock_res(filepath, f.zk_conn)
		if err != nil {
			return nil, err
		}
		return f, nil

	case "RWLock":
		var err error
		f := new(fs_client_rwlock)
		f.zk_servers = append(f.zk_servers, zk_servers...)
		f.backends = append(f.backends, backends...)
		f.zk_conn, _, err = go_zk.Connect(zk_servers, 3*time.Second)
		if err != nil {
			return nil, err
		}

		f.rwl, err = Create_rw_lock_res(filepath, f.zk_conn)
		if err != nil {
			return nil, err
		}
		return f, nil
	}

	return nil, errors.New("Unknown lock config")
}

type Fs_handle interface {
	Read_op(filename string, op []string) (string, error)
	Write(filename string, contents []byte) (*Metadata, error)
	Write_meta(filename string, meta *Metadata) error
	Read_lock(filename string) error
	Read_unlock(filename string) error
	Close()
}


func (f *fs_client_rcu) Read_op(filename string, op []string) (string, error) {
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

func (f *fs_client_rcu) Write(filename string, contents []byte ) (*Metadata, error) {

	meta := new(Metadata)
	for _, sh := range divide_into_shards(contents) {

		c := Nwfs_rpc_client{Backends: get_backends(f.backends, NUM_BACK)}

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

func (f *fs_client_rcu) Write_meta(filename string, meta *Metadata) error {

	meta_bytes, err := json.Marshal(meta)
	if err != nil {
		log.Println("json.Marshal failed: ", err)
		return err
	}

	return f.zk_rcu.Assign(meta.Version, meta_bytes)
}

func (f *fs_client_rcu) Read_lock(filename string) error {
	return f.zk_rcu.Read_lock()
}

func (f *fs_client_rcu) Read_unlock(filename string) error {
	return f.zk_rcu.Read_unlock()
}

// can we reuse the same zookeeper connection for muliple files.
// TODO remove Close
func (f *fs_client_rcu)Close() {
	if verbose_logs {
		log.Println("Close the zookeeper connection")
	}
	f.zk_conn.Close()
}

// helpers
// get a few backends to init
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


