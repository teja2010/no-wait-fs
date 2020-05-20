package main

import (
	"os"
	"fmt"
	"net"
	"errors"
	"net/rpc"
	"net/http"
)

func print_help() {
	fmt.Println("Usage: nwfs_back <port>")
}

func main() {
	args := os.Args[1:]

	if len(args) != 1 {
		print_help()
		os.Exit(1)
	}
	port := args[0]
	n := new(nwfs)

	n.prefix = "shards_"+port
	e := os.Mkdir(n.prefix, os.ModeDir)
	if e != nil {
		fmt.Println("Mkdir error", e)
		os.Exit(1)
	}

	rpc.RegisterName("No_Wait_FS server", n)
	rpc.HandleHTTP()

	l, e := net.Listen("tcp", ":" + port)
	if e != nil {
		fmt.Println("Listen error", e)
		os.Exit(1)
	}

	http.Serve(l, nil)
}

type nwfs struct {
	prefix string
	// nothing for now
}

type rpc_iface interface {
	WriteShard(contents string, ret *string) error
	Read_op(args *ReadArgs, ret *string) error
}

func (n *nwfs) WriteShard(contents string, hash *string) error {
	hash = nil
	return errors.New("yet to implement")
}

type ReadArgs struct {
	hash string
	op string
}

func (n *nwfs) Read_op(args *ReadArgs, op_output *string) error {
	return errors.New("yet to implement")
}

