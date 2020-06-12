package nwfslib

import (
	"errors"
	"time"
	"log"
	go_zk "github.com/samuel/go-zookeeper/zk"
	"zk_rculib"
)

const (
	NUM_BACK = 1 // Not handling multiple replicas
	SHARD_SIZE = 8*1024*1024
	VERBOSE_LOGS = false
	ENTRY_ARG_LOGS = false
)

type fs_client_rcu struct {
	zk_servers []string
	backends   []string
	zk_conn    *go_zk.Conn

	zk_rcu     zk_rculib.Zk_RCU_res
}

type Metadata struct {
	Version int32
	Shards []string
	Backs [][]string
}

func Open(filepath string, zk_servers, backends []string, locking string) (Fs_handle, error) {

	if ENTRY_ARG_LOGS {
		log.Println(__FUNC__(), filepath, locking)
	}

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




