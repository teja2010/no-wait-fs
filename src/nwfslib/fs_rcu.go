package nwfslib

import (
	//"errors"
	"log"
	//"time"
	//"math/rand"
	//go_zk "github.com/samuel/go-zookeeper/zk"
	//"zk_rculib"
	"encoding/json"
)

func (f *fs_client_rcu) Read_op(filename string, op []string) (int32, string, error) {
	ver, meta_bytes, err := f.zk_rcu.Dereference()
	if err != nil {
		return -1, "", err
	}
	out, err := Read_op(meta_bytes, op)

	return ver, out, err
}

func (f *fs_client_rcu) Write(filename string, contents []byte ) (*Metadata, error) {
	return Write_shards(filename, contents, f.backends)
}

func (f *fs_client_rcu) Write_op(filename string, op []string) (int32, *Metadata, error) {
	ver, meta_bytes, err := f.zk_rcu.Dereference()
	if err != nil {
		return -1, nil, err
	}

	m, err :=  Write_op_shards(meta_bytes, op)
	return ver, m, err
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
	if VERBOSE_LOGS {
		log.Println("Close the zookeeper connection")
	}
	f.zk_conn.Close()
}
