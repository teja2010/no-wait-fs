package main

import (
	"os"
	"fmt"
	"net"
	"log"
	"errors"
	"os/exec"
	"net/rpc"
	"net/http"
	"nwfslib"
	"io/ioutil"
)

const (
	verbose_logs = false
	success_logs = false
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

	n.prefix = "shards_"+port+"/"
	e := os.Mkdir(n.prefix, os.ModePerm)
	if os.IsExist(e) {
		log.Println("Note:", e)
	} else if e != nil {
		log.Println("Mkdir error", e)
		os.Exit(1)
	}

	rpc.RegisterName("NWFS", n)
	rpc.HandleHTTP()

	l, e := net.Listen("tcp", ":" + port)
	if e != nil {
		log.Println("Listen error", e)
		os.Exit(1)
	}
	
	if verbose_logs {
		log.Println("Start serving")
	}

	http.Serve(l, nil)
}

type nwfs struct {
	prefix string
	// nothing for now
}


type rpc_iface interface {
	// write the shard contents to a localfile and return it's hash.
	// this hash can be used to identify the shard
	WriteShard(contents []byte, hash *string) error

	// given the hash and the operation, the operation's output will be
	// returned in op_output
	Read_op(args *ReadArgs, op_output *string) error
}

func (n *nwfs) WriteShard(contents []byte, hash *string) error {

	if verbose_logs {
		log.Println("WriteShard Entry:", contents, hash)
	}

	if len(contents) == 0 || hash == nil {
		return errors.New("Invalid RPC args")
	}

	*hash = nwfslib.Shard_hash(contents)

	filepath := n.prefix + *hash
	exists, err := does_file_exist(filepath)
	if err != nil {
		log.Println("WriteShard failed", err)
		return err
	} else if exists == true {
		log.Println("File exists.")
		return nil
	}

	err = ioutil.WriteFile(filepath, contents, 0644)
	if err != nil {
		log.Println("WriteShard failed", err)
		return err
	}

	if success_logs {
		log.Println("Writing", filepath, "successful")
	} else {
		fmt.Printf("r")
	}

	return nil
}

type ReadArgs struct {
	Hash string
	Op []string // all gaps will be filled with the file path
		    // e.g. ["grep LOG", "| grep error"]
		    //  runs: grep LOG <file_name> | grep error
}

func (n *nwfs) Read_op(args *ReadArgs, op_output *string) error {

	filepath := n.prefix + args.Hash
	exists, err := does_file_exist(filepath)
	if err != nil {
		log.Println("Read_op failed", err)
		return err
	}
	if !exists {
		log.Println("Read_op failed, file does not exist")
		return errors.New("File does not exist")
	}

	op := args.Op[0]
	for _, c := range args.Op[1:] {
		op = op + " " + filepath + " " + c
	}

	if verbose_logs {
		log.Println("Read_op op:", op)
	}

	cmd := exec.Command("sh", "-c", op)
	output, err := cmd.CombinedOutput()

	// TODO: check if command ran but failed OR
	//       the command did not run
	if err != nil {
		log.Println("Read_op failed", err)
		return nil
	}

	if success_logs {
		log.Println("Run op:", op)
		log.Println("Output:", output)
	} else {
		fmt.Printf("r")
	}

	*op_output = string(output)
	return nil

}


func does_file_exist(filepath string) (bool, error) {
	_, err := os.Stat(filepath)
	if err == nil {
		return true, nil
	} else if !os.IsNotExist(err) {
		return false, err
	}

	return false, nil
}
