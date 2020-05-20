package main

import (
	"fmt"
	"os"
	//"io"
	"time"
	"io/ioutil"
	"encoding/json"
	"../nwfslib"
)

func print_help() {
	fmt.Println("Usage: nwfs_client <config_file>")
	os.Exit(1);
}

type Config struct {
	Zk_servers []string
	Back_servers []string
	Periodic int
	File_size uint64
}

func read_config(filename string) Config {
	jd, err := os.Open(filename)
	if err != nil {
		fmt.Println("Open error", err)
		os.Exit(1)
	}

	jsonData, err := ioutil.ReadAll(jd)
	if err != nil {
		fmt.Println("Readall error", err)
		os.Exit(1)
	}

	var config Config
	err = json.Unmarshal(jsonData, &config)
	if err != nil {
		fmt.Println("Config unmarshall failed")
		os.Exit(1)
	}

	return config
}

func main() {
	args := os.Args[1:]

	if len(args) != 1 {
		print_help()
	}

	config := read_config(args[0])
	fmt.Println("Config ", config)

	_, _ = nwfslib.New_client(config.Zk_servers, config.Back_servers)

	for {
		send_update(config.File_size)
		fmt.Println("--- Wait for", config.Periodic, "seconds ---")
		time.Sleep(time.Duration(config.Periodic)*time.Second)
	}
}

func send_update(file_size uint64) {
	fmt.Println("File size", file_size)
}

