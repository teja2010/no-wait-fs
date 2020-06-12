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

	if err == go_zk.ErrNodeExists {
		if VERBOSE_LOGS {
			log.Println("Node exists")
		}
	} else if err != nil {
		log.Println("Create_rw_lock_res err:", err)
	}

	rwl, err := zk_rwllib.Create_RWLock(filepath+"/rwlock", zk)

	return rwl, err;
}

func (frwl *fs_client_rwlock) Read_op(filename string, op []string) (int32, string, error) {
	zk := frwl.zk_conn
	meta_bytes, stat, err := zk.Get("/"+filename)
	if err != nil {
		return -1, "", err
	}
	m, err := Read_op(meta_bytes, op)

	return stat.Version, m, err
}
func (frwl *fs_client_rwlock) Write_op(filename string, op []string) (int32, *Metadata, error) {
	zk := frwl.zk_conn
	meta_bytes, stat, err := zk.Get("/"+filename)
	if err != nil {
		return -1, nil, err
	}

	out, err := Write_op_shards(meta_bytes, op)

	return stat.Version, out, err
}

func (frwl *fs_client_rwlock) Write(filename string, contents []byte) (*Metadata, error) {
	return Write_shards(filename, contents, frwl.backends)
}

func (frwl *fs_client_rwlock) Write_meta(filename string, meta *Metadata) error {
	frwl.rwl.WriteLock()
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
	if VERBOSE_LOGS {
		log.Println("Close the zookeeper connection")
	}
	frwl.zk_conn.Close()
}
