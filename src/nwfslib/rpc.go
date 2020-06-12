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

type Nwfs_rpc_client struct {
	Backends []string //array of len NUM_BACK
	clients []*rpc.Client
}

func (nrc *Nwfs_rpc_client) Connect() error {

	nrc.clients = make([]*rpc.Client, len(nrc.Backends))

	nrc.clients = make([]*rpc.Client, len(nrc.Backends))
	for i, b := range nrc.Backends {
		c, err := rpc.DialHTTP("tcp", b)
		if err != nil {
			return err
		}
		nrc.clients[i] = c
	}
	if VERBOSE_LOGS {
		log.Println("rpc Connect:", nrc.clients)
	}
	return nil
}

func (nrc *Nwfs_rpc_client) WriteShard( contents []byte,
					hash *string) (int, error) {
	*hash = ""
	errCnt := 0
	var retErr error
	for i:= 0; i<NUM_BACK; i++ {
		chash := ""
		c := nrc.clients[i]
		err := c.Call("NWFS.WriteShard", contents, &chash)
		if err != nil {
			log.Println("Write Failed")
			errCnt++
			retErr = err
		}
		if *hash == "" {
			*hash = chash
		} else if *hash != chash {
			log.Println("Different values of hash")
		}
	}

	if errCnt > 0 {
		return errCnt, retErr
	} else {
		if VERBOSE_LOGS {
			if len(contents) < 10 {
				log.Println(__FUNC__(), hash, "|", contents)
			} else {
				log.Println(__FUNC__(), hash, "|truncated", contents[:20])
			}
		}
		return 0, nil
	}
}


//type ReadArgs nwfslib.ReadArgs
func (nrc *Nwfs_rpc_client) Read_op(args *ReadArgs,
				    op_output *string) (error) {

	if ENTRY_ARG_LOGS {
		log.Println(__FUNC__(), args)
	}
	var err error
	*op_output = ""
	for i:= 0; i<NUM_BACK; i++ {
		out := ""
		c := nrc.clients[i]
		err = c.Call("NWFS.Read_op", args, &out)
		if err != nil {
			continue
		}

		*op_output = out
		return nil
	}

	return err
}

func (nrc *Nwfs_rpc_client) Close() {
	for i := range nrc.clients {
		nrc.clients[i].Close()
	}
}
