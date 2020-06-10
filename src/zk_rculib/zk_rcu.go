package zk_rculib

import (
	"log"
	"errors"
	"strconv"
	"strings"
	go_zk "github.com/samuel/go-zookeeper/zk"
)

const (
	verbose_logs = true
)

// the main recipe for a RCU sync on zookeeper
// needs to be filled in.

type Zk_RCU_res interface {
	Read_lock() error
	Read_unlock() error
	Assign(version int32, metadata []byte) error
	Dereference() ([]byte, error)
}

type rcu_data struct {
	resource_name string // name of the resource protected by RCU
	Zk *go_zk.Conn
	rlock_file string
	wlock *go_zk.Lock
}

func latest_version(children []string) (int, string) {
	version := -1
	ret := ""
	for _, ch := range children {
		vstr := strings.TrimPrefix(ch, "version")
		v, err := strconv.Atoi(vstr)
		if err != nil {
			continue
		}
		if v > version {
			version = v
			ret = vstr
		}
	}

	if verbose_logs {
		log.Println("Latest Version ", ret)
	}

	return version, ret
}

func Create_RCU_resource(resource_name string,
				zk *go_zk.Conn) (Zk_RCU_res, error) {
	zk_rcu := new(rcu_data)
	zk_rcu.resource_name = resource_name
	zk_rcu.Zk = zk
	zk_rcu.wlock = go_zk.NewLock(zk,
					 "/"+resource_name+"/writelock",
					 go_zk.WorldACL(go_zk.PermAll))

	res_exists, _, err := zk.Exists("/"+resource_name)
	if err != nil {
		return nil, err
	}
	if res_exists {
		if verbose_logs {
			log.Println("Resource Exists")
		}
		return zk_rcu, nil
	}

	ops := []interface{}{
		&go_zk.CreateRequest{ Path: "/" + resource_name,
				      Data: []byte{},
				      Acl: go_zk.WorldACL(go_zk.PermAll),
				      Flags: 0 },

		&go_zk.CreateRequest{ Path: "/" + resource_name + "/writelock",
				      Data: []byte{},
				      Acl: go_zk.WorldACL(go_zk.PermAll),
				      Flags: 0 },

		&go_zk.CreateRequest{ Path: "/" + resource_name + "/version",
				      Data: []byte{},
				      Acl: go_zk.WorldACL(go_zk.PermAll),
				      Flags: go_zk.FlagSequence },
		}

	//Run one or run all
	mresp, err := zk.Multi(ops...)
	if err != nil {
		return nil, err
	}

	for _, mr := range mresp {
		if mr.Error != nil {
			return nil, err
		}
	}

	log.Println("Created RCU resource ", mresp[0].String)

	if verbose_logs {
		log.Println("RCU res initial version", mresp[2].String)
	}

	return zk_rcu, nil

}

func (zk_rcu *rcu_data) Read_lock() error {
	if zk_rcu.rlock_file != "" {
		return errors.New("Deadlock: lock acquired: " + zk_rcu.rlock_file)
	}

	zk := zk_rcu.Zk

	children, _, err := zk.Children("/" + zk_rcu.resource_name)
	if err != nil {
		return err
	}
	latest,lstr := latest_version(children)
	if latest == 0 {
		return errors.New("Resource is uninitialized.")
	}

	path := ("/" + zk_rcu.resource_name + "/version" +
			lstr + "/readlock" )

	if verbose_logs {
		log.Println("Read_lock path: ", path)
	}

	zk_rcu.rlock_file, err = zk.Create(path, []byte{},
					   go_zk.FlagSequence|go_zk.FlagEphemeral,
					   go_zk.WorldACL(go_zk.PermAll))

	if err != nil {
		zk_rcu.rlock_file = ""
		return err
	}

	if verbose_logs {
		log.Println("Lock acquired: ", zk_rcu.rlock_file)
	}

	return nil
}

func (zk_rcu *rcu_data) Read_unlock() error {
	if zk_rcu.rlock_file == "" {
		return errors.New("Lock not acquired")
	}

	zk := zk_rcu.Zk

	err := zk.Delete(zk_rcu.rlock_file, -1)
	if verbose_logs {
		if err != nil {
			log.Println("Read_unlock", zk_rcu.rlock_file,
					"Unsuccesful")
		} else {
			log.Println("Read_unlock", zk_rcu.rlock_file,
					"Succesful")
		}
	}
	zk_rcu.rlock_file = ""
	return err
}

func (zk_rcu *rcu_data) Assign(version int32, metadata []byte) error {
	err := zk_rcu.writer_lock()
	if err != nil {
		return err
	}
	defer zk_rcu.writer_unlock()
	zk := zk_rcu.Zk

	if verbose_logs {
		log.Println("Assign version:", version)
	}

	if version >= 0 {
		children, _, err := zk.Children("/" + zk_rcu.resource_name)
		if err != nil {
			return err
		}
		latest, _ := latest_version(children)

		if int32(latest) != version {
			return errors.New("Mismatching versions")
		}
	}

	new_version, err := zk.Create("/" + zk_rcu.resource_name + "/version",
					metadata, go_zk.FlagSequence,
					go_zk.WorldACL(go_zk.PermAll))
	if err != nil {
		log.Println("Assign: Create failed err:", err)
		return err
	}

	log.Println("Assigned metadata", metadata,
		    ", Created ", new_version)
	return nil
}

func (zk_rcu *rcu_data) Dereference() ([]byte, error) {
	zk := zk_rcu.Zk
	if zk_rcu.rlock_file == "" {
		return nil, errors.New("Dereference outside read_lock protection")
	}

	idx := strings.LastIndex(zk_rcu.rlock_file, "/readlock")
	metadata, _, err := zk.Get(zk_rcu.rlock_file[:idx])
	if err != nil {
		return nil, err
	}
	log.Println("Dereferenced", zk_rcu.rlock_file[:idx],
		    ", Metadata", metadata)
	return metadata, nil
}

func (zk_rcu *rcu_data) writer_lock() error {
	// just use the locks provided by the library
	return zk_rcu.wlock.Lock()
}

func (zk_rcu *rcu_data) writer_unlock() error {
	return zk_rcu.wlock.Unlock()
}

