package nwfslib

import (
	"log"
	go_zk "github.com/samuel/go-zookeeper/zk"
	"encoding/json"
	"zk_rwllib"
)

type fs_client_rwlock struct {
	zk_servers []string
	backends   []string
	zk_conn    *go_zk.Conn
	
	rwl        zk_rwllib.Zk_RWLock
}

func Create_rw_lock_res(filepath string, zk *go_zk.Conn) (zk_rwllib.Zk_RWLock, error) {
	_, err := zk.Create("/"+filepath, []byte{},
			    0, go_zk.WorldACL(go_zk.PermAll))

	if err != nil {
		log.Println("Create_rw_lock_res err:", err)
	}

	rwl, err := zk_rwllib.Create_RWLock(filepath, zk)

	return rwl, err;
}

func (frwl *fs_client_rwlock) Read_op(filename string, op []string) (string, error) {
	zk := frwl.zk_conn
	meta_bytes, _, err := zk.Get("/"+filename)
	if err != nil {
		return "", err
	}

	return Read_op(meta_bytes, op)
}

func (frwl *fs_client_rwlock) Write(filename string, contents []byte) (*Metadata, error) {
	frwl.rwl.WriteLock()
	return write_shards(filename, contents, frwl.backends)
}

func (frwl *fs_client_rwlock) Write_meta(filename string, meta *Metadata) error {
	defer frwl.rwl.WriteUnlock()

	meta_bytes, err := json.Marshal(meta)
	if err != nil {
		log.Println("json.Marshal failed: ", err)
		return err
	}

	_, err = frwl.zk_conn.Set("/"+filename, meta_bytes, meta.Version)
	return err
}

func (frwl *fs_client_rwlock) Read_lock(filename string) error {
	return frwl.rwl.ReadLock()
}

func (frwl *fs_client_rwlock) Read_unlock(filename string) error {
	return frwl.rwl.ReadUnlock()
}

func (frwl *fs_client_rwlock) Close() {
	frwl.zk_conn.Close()
}
