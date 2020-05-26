package nwfslib

import (
	"log"
	"net/rpc"
)


//type rpc_iface interface {
//	// write the shard contents to a localfile and return it's hash.
//	// this hash can be used to identify the shard
//	WriteShard(contents []byte, hash *string) error
//
//	// given the hash and the operation, the operation's output will be
//	// returned in op_output
//	Read_op(args *ReadArgs, op_output *string) error
//}

type nwfs_rpc_client struct {
	Backends []string //array of len NUM_BACK
	clients []*rpc.Client
}

func (nrc *nwfs_rpc_client) Connect() error {
	nrc.clients = make([]*rpc.Client, NUM_BACK)
	for i:=0; i<NUM_BACK; i++ {
		c, err := rpc.DialHTTP("tcp", nrc.Backends[i])
		if err != nil {
			return err
		}
		nrc.clients = append(nrc.clients, c)
	}
	return nil
}

func (nrc *nwfs_rpc_client) WriteShard( contents []byte,
					hash *string) (int, error) {
	*hash = ""
	errCnt := 0
	for i:= 0; i<NUM_BACK; i++ {
		chash := ""
		c := nrc.clients[i]
		err := c.Call("WriteShard", contents, &chash)
		if err != nil {
			errCnt++
		}
		if *hash == "" {
			*hash = chash
		} else if *hash != chash {
			log.Println("Different values of hash")
		}
	}

	return errCnt, nil
}

type ReadArgs struct {
	hash string
	op []string // all gaps will be filled with the file path
		    // e.g. ["grep LOG", "| grep error"]
		    //  runs: grep LOG <file_name> | grep error
}

func (nrc *nwfs_rpc_client) Read_op(args *ReadArgs,
				    op_output *string) (error) {
	var err error
	*op_output = ""
	for i:= 0; i<NUM_BACK; i++ {
		out := ""
		c := nrc.clients[i]
		err = c.Call("WriteShard", args, &out)
		if err != nil {
			continue
		}

		*op_output = out
		return nil
	}

	return err
}

func (nrc *nwfs_rpc_client) Close() {
	for i:=0; i<NUM_BACK; i++ {
		nrc.clients[i].Close()
	}
}
