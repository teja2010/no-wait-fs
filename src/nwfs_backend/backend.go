package main

import (
	"os"
	"fmt"
	"net"
	"log"
	"time"
	"errors"
	"os/exec"
	"net/rpc"
	"net/http"
	"math/rand"
	"nwfslib"
	"io/ioutil"
	"zk_rculib"
	"encoding/json"
	go_zk "github.com/samuel/go-zookeeper/zk"
)

type Config struct {
	Zk_servers []string
}

const (
	VERBOSE_LOGS = false
	SUCCESS_LOGS = false
)

func print_help() {
	fmt.Println("Usage: nwfs_back <port> <config>")
}

func read_config(filename string) Config {
	jd, err := os.Open(filename)
	if err != nil {
		log.Println("Open error", err)
		os.Exit(1)
	}
	defer jd.Close()

	jsonData, err := ioutil.ReadAll(jd)
	if err != nil {
		log.Println("Readall error", err)
		os.Exit(1)
	}

	var config Config
	err = json.Unmarshal(jsonData, &config)
	if err != nil {
		log.Println("Config unmarshall failed", err)
		os.Exit(1)
	}

	log.Printf("config: %+v\n", config)

	return config
}

func rcu_gc(Zk_servers []string) {
	zk, _, err := go_zk.Connect(Zk_servers, 3*time.Second)

	for err == nil {
		time.Sleep(time.Duration(5+rand.Intn(5))*time.Second)
		err = zk_rculib.Rcu_gc(zk)
	}

	log.Println("Zk Connection failed", err)
	time.Sleep(3*time.Second)
	go rcu_gc(Zk_servers)
	return
}

func main() {
	args := os.Args[1:]

	if len(args) != 2 {
		print_help()
		os.Exit(1)
	}
	port := args[0]

	rand.Seed(time.Now().UnixNano())
	log.SetFlags(log.LstdFlags|log.Lshortfile)
	c := read_config(args[1])

	go rcu_gc(c.Zk_servers)

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
	
	if VERBOSE_LOGS {
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

	Write_op_shards(args *ReadArgs, hash *string) error
}


func (n *nwfs) WriteShard(contents []byte, hash *string) error {

	if VERBOSE_LOGS {
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
		if VERBOSE_LOGS {
			log.Println("File exists.")
		} else {
			fmt.Printf("F")
		}
		return nil
	}

	err = ioutil.WriteFile(filepath, contents, 0644)
	if err != nil {
		log.Println("WriteShard failed", err)
		return err
	}

	if SUCCESS_LOGS {
		log.Println("Writing", filepath, "successful")
	} else {
		fmt.Printf("w")
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
		//log.Println("Read_op failed, file does not exist")
		return errors.New("File does not exist")
	}

	op := args.Op[0]
	for _, c := range args.Op[1:] {
		op = op + " " + filepath + " " + c
	}

	if VERBOSE_LOGS {
		log.Println("Read_op op:", op)
	}

	cmd := exec.Command("sh", "-c", op)
	output, err := cmd.CombinedOutput()

	// TODO: check if command ran but failed OR
	//       the command did not run
	if err != nil {
		//log.Println("Read_op failed", err)
		return nil
	}

	if SUCCESS_LOGS {
		log.Println("Run op:", op)
		log.Println("Output:", output)
	} else {
		fmt.Printf("r")
	}

	*op_output = string(output)
	return nil

}

func (n *nwfs) Write_op_shards(args *ReadArgs, hash *string) error {

	out := ""
	err := n.Read_op(args, &out)
	if err != nil {
		return err
	}

	err = n.WriteShard([]byte(out), hash)
	if err == nil {
		fmt.Printf("u")
	}

	return err
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
