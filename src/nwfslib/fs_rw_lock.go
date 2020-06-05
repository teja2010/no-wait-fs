package nwfslib

import (
	"log"
	go_zk "github.com/samuel/go-zookeeper/zk"
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
	return "", nil
}
func (frwl *fs_client_rwlock) Write(filename string, contents []byte) (*Metadata, error) {
	return nil, nil
}
func (frwl *fs_client_rwlock) Write_meta(filename string, meta *Metadata) error {
	return nil
}
func (frwl *fs_client_rwlock) Read_lock(filename string) error {
	return nil
}
func (frwl *fs_client_rwlock) Read_unlock(filename string) error {
	return nil
}
func (frwl *fs_client_rwlock) Close() {

}
