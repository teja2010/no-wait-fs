package nwfslib

import (
	"log"
	"encoding/json"
	go_zk "github.com/samuel/go-zookeeper/zk"
)

type fs_client_nolock struct {
	zk_servers []string
	backends   []string
	zk_conn    *go_zk.Conn

	nl       string
}

func Create_no_lock_res(filepath string, zk *go_zk.Conn) (string, error) {
	res_exists, _, err := zk.Exists("/"+filepath)

	var nl string
	if !res_exists {
		nl, err = zk.Create("/"+filepath, []byte{},
					0, go_zk.WorldACL(go_zk.PermAll))
	}

	log.Println("Created a nolock resource")
	return nl, err
}

func (fnl *fs_client_nolock) Read_op(filename string, op []string) (string, error) {

	zk := fnl.zk_conn
	path := "/" + filename + "/nolock_data"
	meta_bytes, _, err := zk.Get(path)
	if err != nil {
		return "", err
	}

	return Read_op(meta_bytes, op)
}
func (fnl *fs_client_nolock) Write(filename string, contents []byte) (*Metadata, error) {
	return write_shards(filename, contents, fnl.backends)
}
func (fnl *fs_client_nolock) Write_meta(filename string, meta *Metadata) error {
	zk := fnl.zk_conn
	meta_bytes, err := json.Marshal(meta)
	if err != nil {
		log.Println("json.Marshal failed: ", err)
		return err
	}

	if VERBOSE_LOGS {
		log.Println(__FUNC__(), "meta_bytes: ", meta_bytes)
	}

	path := "/" + filename + "/nolock_data"
	_, err = zk.Set(path, meta_bytes, -1)

	if err != nil {
		_, err = zk.Create(path, meta_bytes, 0,
				   go_zk.WorldACL(go_zk.PermAll))
	}
	
	if err != nil {
		log.Println("Write_meta: failed", err)
		return err
	}
	return nil
}

func (fnl *fs_client_nolock) Read_lock(filename string) error {
	return nil
}
func (fnl *fs_client_nolock) Read_unlock(filename string) error {
	return nil
}
func (fnl *fs_client_nolock) Close() {
	if VERBOSE_LOGS {
		log.Println("Close the zookeeper connection")
	}
	fnl.zk_conn.Close()
}

