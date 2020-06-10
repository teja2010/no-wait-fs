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

func (f *fs_client_rcu) Read_op(filename string, op []string) (string, error) {
	meta_bytes, err := f.zk_rcu.Dereference()
	if err != nil {
		return "", err
	}

	return Read_op(meta_bytes, op)
}

func (f *fs_client_rcu) Write(filename string, contents []byte ) (*Metadata, error) {
	return write_shards(filename, contents, f.backends)
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
