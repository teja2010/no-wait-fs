package zk_rculib

import (
	"fmt"
	"log"
	"errors"
	"strconv"
	"strings"
	go_zk "github.com/samuel/go-zookeeper/zk"
)

const (
	VERBOSE_LOGS = false
)

// the main recipe for a RCU sync on zookeeper
// needs to be filled in.

type Zk_RCU_res interface {
	Read_lock() error
	Read_unlock() error
	Assign(version int32, metadata []byte) error
	Dereference() (int32, []byte, error)
}

type rcu_data struct {
	resource_name string // name of the resource protected by RCU
	Zk *go_zk.Conn
	rlock_file string
	read_version int32
	wlock *go_zk.Lock
	//wchan chan int
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

	if VERBOSE_LOGS {
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
		if VERBOSE_LOGS {
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
	if err == go_zk.ErrNodeExists {
		if VERBOSE_LOGS {
			log.Println("Node exists")
		}
	} else if err != nil {
		if VERBOSE_LOGS {
			log.Println("Multi err", err)
		}
		return nil, err
	}

	// check this part
	//for _, mr := range mresp {
	//	log.Println(mr)
	//	if mr.Error == go_zk.ErrNodeExists {
	//		log.Println("err3", mr.Error) // make verbose
	//		continue
	//	} else {
	//		if mr.Error != nil {
	//			log.Println("err2", mr.Error) // make verbose
	//			return nil, mr.Error
	//		}
	//	}
	//}

	log.Println("Created RCU resource ", mresp[0].String)

	if VERBOSE_LOGS {
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

	if VERBOSE_LOGS {
		log.Println("Read_lock path: ", path)
	}

	zk_rcu.rlock_file, err = zk.Create(path, []byte{},
					   go_zk.FlagSequence|go_zk.FlagEphemeral,
					   go_zk.WorldACL(go_zk.PermAll))

	if err != nil {
		zk_rcu.rlock_file = ""
		return err
	}

	if VERBOSE_LOGS {
		log.Println("Lock acquired: ", zk_rcu.rlock_file)
	}
	zk_rcu.read_version = int32(latest)

	return nil
}

func (zk_rcu *rcu_data) Read_unlock() error {
	if zk_rcu.rlock_file == "" {
		return errors.New("Lock not acquired")
	}

	zk := zk_rcu.Zk

	err := zk.Delete(zk_rcu.rlock_file, -1)
	if VERBOSE_LOGS {
		if err != nil {
			log.Println("Read_unlock", zk_rcu.rlock_file,
					"Unsuccesful")
		} else {
			log.Println("Read_unlock", zk_rcu.rlock_file,
					"Succesful")
		}
	}

	zk_rcu.rlock_file = ""
	zk_rcu.read_version = 0


	return err
}

func (zk_rcu *rcu_data) Assign(version int32, metadata []byte) error {
	err := zk_rcu.writer_lock()
	if err != nil {
		return err
	}
	defer zk_rcu.writer_unlock()
	zk := zk_rcu.Zk

	if VERBOSE_LOGS {
		log.Println("Assign version:", version)
	}

	if version >= 0 {
		children, _, err := zk.Children("/" + zk_rcu.resource_name)
		if err != nil {
			return err
		}
		latest, _ := latest_version(children)

		if int32(latest) != version {
			fmt.Printf("(%d != %d)", latest, version)
			return go_zk.ErrBadVersion
		}
	}

	new_version, err := zk.Create("/" + zk_rcu.resource_name + "/version",
					metadata, go_zk.FlagSequence,
					go_zk.WorldACL(go_zk.PermAll))
	if err != nil {
		log.Println("Assign: Create failed err:", err)
		return err
	}

	if VERBOSE_LOGS {
		log.Println("Assigned metadata", metadata,
			    ", Created ", new_version)
	}
	return nil
}

func (zk_rcu *rcu_data) Dereference() (int32, []byte, error) {
	zk := zk_rcu.Zk
	if zk_rcu.rlock_file == "" {
		return -1, nil, errors.New("Dereference outside read_lock protection")
	}

	idx := strings.LastIndex(zk_rcu.rlock_file, "/readlock")
	metadata, _, err := zk.Get(zk_rcu.rlock_file[:idx])
	if err != nil {
		return -1, nil, err
	}
	if VERBOSE_LOGS {
		log.Println("Dereferenced", zk_rcu.rlock_file[:idx],
			    ", Metadata", metadata)
	}

	return zk_rcu.read_version, metadata, nil
}

func (zk_rcu *rcu_data) writer_lock() error {
	// just use the locks provided by the library
	err := zk_rcu.wlock.Lock()
	return err

	//go zk_rcu.rcu_gc()

}

func (zk_rcu *rcu_data) writer_unlock() error {
	//<-zk_rcu.wchan

	return zk_rcu.wlock.Unlock()
}

//func (zk_rcu *rcu_data) rcu_gc() {
//	defer func (){ 0->zk_rcu.wchan }()
//
//	children, _, err := zk.Children("/" + zk_rcu.resource_name)
//	if err != nil {
//		return err
//	}
//	latest, lstr := latest_version(children)
//
//	for _, ch := range children {
//		if ch == lstr {
//			continue
//		}
//
//		children, _, err := zk.Children("/" + zk_rcu.resource_name+
//						"/" + ch)
//		if len(ch) == 0 {
//			_ = zk.Delete(temp, -1)
//		}
//	}
//
//}
