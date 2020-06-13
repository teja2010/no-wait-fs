package zk_rwllib

import (
	"log"
	"errors"
	"strconv"
	"strings"
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
	myseq int
	Zk *go_zk.Conn

	lock_file string
}

func Create_RWLock(filepath string, zk *go_zk.Conn) (Zk_RWLock, error) {

	rw := new(rwlock)
	rw.path = filepath
	rw.Zk = zk

	log.Println("Created rwlock at ", filepath)
	_, err := zk.Create("/" + rw.path, []byte{}, 0,
				go_zk.WorldACL(go_zk.PermAll))
	if err != nil && err != go_zk.ErrNodeExists {
		return nil, err
	}

	return rw, nil
}

func (rw *rwlock) ReadLock() error {
	if rw.lock_file != "" {
		log.Println("Deadlock: lock acquired: " + rw.lock_file);
		return go_zk.ErrDeadlock
	}
	var err error
	zk := rw.Zk

	rw.lock_file , err = zk.Create("/" + rw.path + "/reader", []byte{},
					go_zk.FlagSequence | go_zk.FlagEphemeral,
					go_zk.WorldACL(go_zk.PermAll))
	if err != nil {
		return err
	}


	splits := strings.Split(rw.lock_file, "/")
	lk_filename := splits[len(splits)-1]
	rw.myseq, err = strconv.Atoi(strings.Trim(lk_filename, "reader"))
	if err != nil {
		log.Println("Unable to extract sequence num")
		return err
	}

	err = rw.wait_for("writer")
	return err
}

func (rw *rwlock) ReadUnlock() error {
	if rw.lock_file == "" {
		log.Println("Lock not acquired")
		return go_zk.ErrNotLocked
	}

	zk := rw.Zk
	err := zk.Delete(rw.lock_file, -1)
	rw.lock_file = ""
	return err
}
func (rw *rwlock) WriteLock() error {
	if rw.lock_file != "" {
		log.Println("Deadlock: lock acquired: " + rw.lock_file);
		return go_zk.ErrDeadlock
	}

	zk := rw.Zk

	var err error
	rw.lock_file, err = zk.Create("/" + rw.path + "/writer", []byte{},
					go_zk.FlagSequence | go_zk.FlagEphemeral,
					go_zk.WorldACL(go_zk.PermAll))
	if err != nil {
		return err
	}

	splits := strings.Split(rw.lock_file, "/")
	lk_filename := splits[len(splits)-1]
	rw.myseq, err = strconv.Atoi(strings.Trim(lk_filename, "writer"))
	if err != nil {
		log.Println("Unable to extract sequence num")
		return err
	}

	//err = rw.wait_for("writer") // wait till we are the latest writer
	//if err != nil {
	//	return err
	//}

	err = rw.wait_for("reader") // wait for other readers to complete
	return err
}

func (rw *rwlock) WriteUnlock() error {
	if rw.lock_file == "" {
		log.Println("Lock not acquired")
		return go_zk.ErrNotLocked
	}

	zk := rw.Zk
	err := zk.Delete(rw.lock_file, -1)
	rw.lock_file = ""

	return err
}

func (rw *rwlock) wait_for(node_type string) error {
	zk := rw.Zk

	if node_type != "writer" && node_type != "reader" {
		return errors.New("Unknown "+node_type)
	}

	for {
		children, _, err := zk.Children("/"+ rw.path)
		if err != nil {
			return err
		}

		prev := "";
		ver := -1

		// ver is the node just before self
		// i.e. v < myseq

		for _, ch := range children {
			v, err := strconv.Atoi(strings.TrimPrefix(ch, node_type))
			if err != nil {
				continue
			}
			if v < rw.myseq {
				if ver == -1 {
					ver = v
					prev = ch

				// myseq < v < ver
				} else if ver < v {
					ver = v
					prev = ch
				}
			}
		}
		if prev == "" { // no one before you
			return nil
		}
		rw.__wait_for_node(prev)
	}

}

func (rw *rwlock) __wait_for_node(name string) {

	zk := rw.Zk
	exists, _, watch_chan, err  := zk.ExistsW("/" + rw.path + "/" + name)
	if !exists || err != nil {
		return
	}

	ev := <-watch_chan
	if ev.Err != nil {
		return
	}
	//TODO: check if ev.Event is EventNodeDeleted
}
