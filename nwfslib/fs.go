package nwfslib

import (
	"errors"
)

type fsc struct { // fs client
	zk_servers []string
	backends   []string
}

type Metadata struct {
	Version int
	backs [][]string
}

func New_client(zk_servers, backends []string) (*fsc, error) {
	f := new(fsc)
	f.zk_servers = append(f.zk_servers, zk_servers...)
	f.backends = append(f.backends, backends...)

	return f, nil
}

func (f *fsc) Read_op(filename string, op string) (string, error) {

	return "", errors.New("yet to implement")
}

func (f *fsc) Write(filename string, contents string) (*Metadata, error) {
	m := new(Metadata)
	return m, errors.New("yet to implement")
}

func (f *fsc) Write_meta(filename string, meta *Metadata) error {
	return errors.New("yet to implement")
}

func (f *fsc) Read_lock(filename string) error {
	return errors.New("yet to implement")
}

func (f *fsc) Read_unlock(filename string) error {
	return errors.New("yet to implement")
}

