package nwfslib

import (
	"log"
	"encoding/json"
	go_zk "github.com/samuel/go-zookeeper/zk"
)

type fs_client_singleLock struct {
	zk_servers []string
	backends   []string
	zk_conn    *go_zk.Conn

	lock       *go_zk.Lock
}

func Create_single_lock_res(filepath string, zk *go_zk.Conn) (*go_zk.Lock, error) {

	_, err := zk.Create("/"+filepath, []byte{},
			    0, go_zk.WorldACL(go_zk.PermAll))
	if err != nil {
		log.Println("Create_single_lock_res: error:",err);
	}

	lock := go_zk.NewLock(zk, "/"+filepath+"/singlelock",
				go_zk.WorldACL(go_zk.PermAll))
	
	return lock, nil
}

func (fsl *fs_client_singleLock) Read_op(filename string, op []string) (string, error) {
	fsl.lock.Lock()
	zk := fsl.zk_conn

	path := "/" + filename + "/singlelock_data"
	meta_bytes, _, err := zk.Get(path)
	if err != nil {
		return "", err
	}

	var meta Metadata
	err = json.Unmarshal(meta_bytes, &meta)
	if err != nil {
		log.Println("json.Unmarshal failed: ", err)
		return "", err
	}

	output := ""
	for i, backs := range meta.Backs {
		shard := meta.Shards[i]
		c := Nwfs_rpc_client{Backends: backs}

		err := c.Connect()
		if err != nil {
			return "", err
		}
		defer c.Close()

		op_output := ""
		err = c.Read_op(&ReadArgs{Hash: shard, Op: op}, &op_output)
		if err != nil {
			return "", err
		}
		
		output += op_output
	}

	return output, nil
}

func (fsl *fs_client_singleLock) Write(filename string, contents []byte) (*Metadata, error) {
	meta := new(Metadata)

	for _, sh := range divide_into_shards(contents) {
		c := Nwfs_rpc_client{Backends: get_backends(fsl.backends, NUM_BACK)}

		err := c.Connect()
		if err != nil {
			return nil, err
		}
		defer c.Close()

		hash := ""
		_, err = c.WriteShard(sh, &hash)
		if err != nil {
			return nil, err
		}

		meta.Shards = append(meta.Shards, hash)
		meta.Backs = append(meta.Backs, c.Backends)
	}

	return meta, nil
}

func (fsl *fs_client_singleLock) Write_meta(filename string, meta *Metadata) error {
	fsl.lock.Lock()
	defer fsl.lock.Unlock()

	zk := fsl.zk_conn
	meta_bytes, err := json.Marshal(meta)
	if err != nil {
		log.Println("json.Marshal failed: ", err)
		return err
	}

	path := "/" + filename + "/singlelock_data"
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

func (fsl *fs_client_singleLock) Read_lock(filename string) error {
	fsl.lock.Lock()
	return nil
}

func (fsl *fs_client_singleLock) Read_unlock(filename string) error {
	fsl.lock.Unlock()
	return nil
}

func (fsl *fs_client_singleLock) Close() {
	if verbose_logs {
		log.Println("Close the zookeeper connection")
	}
	fsl.zk_conn.Close()
}
