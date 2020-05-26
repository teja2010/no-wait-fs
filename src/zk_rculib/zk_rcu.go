package zk_rculib

import (
	"log"
	"errors"
	"strconv"
	"strings"
	go_zk "github.com/samuel/go-zookeeper/zk"
)

// the main recipe for a RCU sync on zookeeper
// needs to be filled in.

type Zk_RCU_res interface {
	Read_lock() error
	Read_unlock() error
	Assign(version int, metadata []byte) error
	Dereference() ([]byte, error)
}

type rcu_data struct {
	resource_name string // name of the resource protected by RCU
	Zk *go_zk.Conn
	rlock_file string
	wlock *go_zk.Lock
}

func latest_version(children []string) (int) {
	version := -1
	for _, ch := range children {
		v, err := strconv.Atoi(strings.Trim(ch, "version"))
		if err != nil {
			continue
		}
		if v > version {
			version = v
		}
	}

	return version
}

// FIXME: use Multi
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
	log.Println("RCU res initial version", mresp[2].String)

	return zk_rcu, nil

	//rs_exists, stat, err := zk.Exists("/"+resource_name)
	//if err != nil {
	//	return nil, err
	//}
	//if !rs_exists {
	//	resrc_path, err = zk.Create("/"+resource_name, []byte{0}, 0, WorldACL(PermRead))
	//	if err != nil {
	//		return nil, err
	//	}
	//	log.Println("Created ", resrc_path)
	//}

	//children, _, err := zk.Children("/"+resource_name)
	//if err != nil {
	//	return nil, err
	//}
	//// atleast should have one version and writelock
	//version_exists := (latest_version(children) >= 0)

	//if !version_exists {
	//	_, err = zk.Create("/"+resource_name+"/version", []byte{0},
	//			   FlagSequence, WorldACL(PermAll))
	//	if err != nil {
	//		return nil, err
	//	}
	//}

	////finally add the write lock folder
	//_, err = zk.Create("/"+resource_name+"/writer_lock", []byte{0},
	//		   FlagSequence, WorldACL(PermAll))
	//if err != nil {
	//	return nil, err
	//}
	//
	//return zk_rcu, nil
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
	latest := latest_version(children)
	if latest == 0 {
		return errors.New("Resource is uninitialized.")
	}

	zk_rcu.rlock_file, err = zk.Create(("/" + zk_rcu.resource_name +
						  "/version" +
						  strconv.Itoa(latest) +
						  "/readlock"),
					     []byte{},
					     go_zk.FlagSequence|go_zk.FlagEphemeral,
					     go_zk.WorldACL(go_zk.PermAll))

	if err != nil {
		zk_rcu.rlock_file = ""
		return err
	}

	log.Println("Lock acquired: ", zk_rcu.rlock_file)
	return nil
}

func (zk_rcu *rcu_data) Read_unlock() error {
	if zk_rcu.rlock_file == "" {
		return errors.New("Lock not acquired")
	}

	zk := zk_rcu.Zk

	return zk.Delete(zk_rcu.rlock_file, -1)
}

func (zk_rcu *rcu_data) Assign(version int, metadata []byte) error {
	err := zk_rcu.writer_lock()
	if err != nil {
		return err
	}
	defer zk_rcu.writer_unlock()
	zk := zk_rcu.Zk


	if version > 0 {
		children, _, err := zk.Children("/" + zk_rcu.resource_name)
		if err != nil {
			return err
		}
		latest := latest_version(children)

		if latest != version {
			return errors.New("Mismatching versions")
		}
	}

	new_version, err := zk.Create("/" + zk_rcu.resource_name + "/version",
					metadata, go_zk.FlagSequence,
					go_zk.WorldACL(go_zk.PermAll))
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




