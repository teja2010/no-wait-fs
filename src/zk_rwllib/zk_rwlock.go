package zk_rwllib

import (
	"log"
	go_zk "github.com/samuel/go-zookeeper/zk"
)

type Zk_RWLock interface {
	ReadLock() error
	ReadUnlock() error

	WriteLock() error
	WriteUnlock() error
}

type rwlock struct {
	path string
	zk *go_zk.Conn

	lock_file string
}

func Create_RWLock(filepath string, zk *go_zk.Conn) (Zk_RWLock, error) {

	rw := new(rwlock)

	log.Println("Created rwlock at ", filepath)

	return rw, nil
}

func (rw *rwlock) ReadLock() error {
	return nil
}
func (rw *rwlock) ReadUnlock() error {
	return nil
}
func (rw *rwlock) WriteLock() error {
	return nil
}
func (rw *rwlock) WriteUnlock() error {
	return nil
}
