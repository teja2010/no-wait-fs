package main

import (
	"log"
	"fmt"
	"os"
	//"io"
	"time"
	"strconv"
	//"math"
	"math/rand"
	"io/ioutil"
	"encoding/json"
	"nwfslib"
)

const (
	BENCH_LOOP_LEN = 200


	hello_lyrics = `Hello, it's me
	I was wondering if after all these years you'd like to meet
	To go over everything
	They say that time's supposed to heal ya
	But I ain't done much healing

	Hello, can you hear me?
	I'm in California dreaming about who we used to be
	When we were younger and free
	I've forgotten how it felt before the world fell at our feet

	There's such a difference between us
	And a million miles

	Hello from the other side
	I must've called a thousand times
	To tell you I'm sorry for everything that I've done
	But when I call, you never seem to be home

	Hello from the outside
	At least I can say that I've tried
	To tell you I'm sorry for breaking your heart
	But it don't matter, it clearly doesn't tear you apart anymore

	Hello, how are you?
	It's so typical of me to talk about myself, I'm sorry
	I hope that you're well
	Did you ever make it out of that town where nothing ever happened?

	It's no secret that the both of us are running out of time

	So hello from the other side
	I must've called a thousand times
	To tell you I'm sorry for everything that I've done
	But when I call, you never seem to be home

	Hello from the outside
	At least I can say that I've tried
	To tell you I'm sorry for breaking your heart
	But it don't matter, it clearly doesn't tear you apart anymore

	Oooooh, anymore
	Oooooh, anymore
	Oooooh, anymore
	Anymore

	Hello from the other side
	I must've called a thousand times
	To tell you I'm sorry for everything that I've done
	But when I call, you never seem to be home

	Hello from the outside
	At least I can say that I've tried
	To tell you I'm sorry for breaking your heart
	But it don't matter, it clearly doesn't tear you apart anymore
	` // adele
)

func print_help() {
	fmt.Println("Usage: nwfs_client <config_file>")
	os.Exit(1);
}

type Config struct {
	Zk_servers []string
	Back_servers []string
	Periodic_ms int
	File_size uint64
	Read_prob float32
	Threads_num int
	Locking string

	Read_ops [][]string

	Test_backend bool
	Test_hello bool
	Test_benchmark_reads bool
	Test_backend_bench bool
	Test_rand_rw_bench bool

}

func read_config(filename string) Config {
	jd, err := os.Open(filename)
	if err != nil {
		log.Println("Open error", err)
		os.Exit(1)
	}

	jsonData, err := ioutil.ReadAll(jd)
	if err != nil {
		log.Println("Readall error", err)
		os.Exit(1)
	}

	var config Config
	err = json.Unmarshal(jsonData, &config)
	if err != nil {
		log.Println("Config unmarshall failed")
		os.Exit(1)
	}

	log.Printf("config: %+v\n", config)

	return config
}

func main() {
	args := os.Args[1:]

	if len(args) != 1 {
		print_help()
	}

	log.SetFlags(log.LstdFlags|log.Lshortfile)

	config := read_config(args[0])

	if config.Test_backend {
		config.call_backend()
	}
	if config.Test_hello {
		config.hello_test()
	}
	if config.Test_benchmark_reads {
		config.read_bench()
	}
	if config.Test_backend_bench {
		config.backend_bench()
	}
	if config.Test_rand_rw_bench {
		for i:=0; i< config.Threads_num; i++ {
			go config.rand_rw_bench()
		}
	}


	for {
		send_update(config.File_size)
		log.Println("--- Wait for", config.Periodic_ms, "Milliseconds ---")
		time.Sleep(time.Duration(config.Periodic_ms)*time.Millisecond)
	}
}

func send_update(file_size uint64) {
	log.Println("File size", file_size)
}

func (c *Config) call_backend() {

	nrc := nwfslib.Nwfs_rpc_client{Backends: c.Back_servers}
	err := nrc.Connect()
	if err != nil {
		log.Println("test_backend Connect err:", err)
		return
	}
	fmt.Println("nrc:", nrc)

	fmt.Println("Commands: Read_op/WriteShard\n")

	for {
		var cmd, val, out string
		fmt.Printf("> ")
		fmt.Scanf("%s %s", &cmd, &val)
		switch cmd {
		case "Read_op":
			err = nrc.Read_op(&nwfslib.ReadArgs{
						Hash: val,
						Op: []string{"grep hello", " "}},
						//Op: []string{"cat ", " "}},
					  &out)
			if err != nil {
				log.Println("Read_op err: ", err)
			} else {
				log.Println("Read_op output", out)
			}
		case "WriteShard":
			var hash string
			_, err = nrc.WriteShard([]byte(val), &hash)
			if err != nil {
				log.Println("WriteShard err: ",err)
			} else {
				log.Println("Wrote to Shard: ", hash)
			}
		default:
			log.Println("Unknown command: ", cmd)
		}
	}
}

func (c* Config) backend_bench() {
	filename := "clientdir"

	var meta *nwfslib.Metadata
	start := time.Now()
	for i:=0; i<BENCH_LOOP_LEN; i++ {
		fmt.Printf(".")
		if i%50 == 49 {
			fmt.Println(i+1)
		}

		var err error
		meta, err = nwfslib.Write_shards(filename,
					[]byte(strconv.Itoa(i) +
						hello_lyrics +
						strconv.Itoa(i)),
					c.Back_servers)
		if err != nil {
			log.Println("Read_op err: ", err)
			return
		}
		if i == 0 {
			log.Println(meta)
		}
	}
	elapsed := time.Now().Sub(start)
	log.Println("Backend Write Bench", elapsed, "/", BENCH_LOOP_LEN, " iters")

	meta_bytes, err := json.Marshal(meta)
	if err != nil {
		log.Println("json.Marshal failed: ", err)
		return
	}

	start = time.Now()
	for i:=0; i<BENCH_LOOP_LEN; i++ {
		fmt.Printf(".")
		if i%50 == 49 {
			fmt.Println(i+1)
		}

		out, err := nwfslib.Read_op(meta_bytes, []string{"grep -i hello ", " "})
		if err != nil {
			log.Println("Read_op err: ", err)
			return
		}
		if i == 0 {
			log.Println(out)
		}
	}
	elapsed = time.Now().Sub(start)
	log.Println("Backend Read Bench", elapsed, "/", BENCH_LOOP_LEN, " iters")


}

func (c *Config) hello_test() {
	var err error
	filename := "clientdir"

	fs, err := nwfslib.Open(filename, c.Zk_servers, c.Back_servers,
			    c.Locking)
	if err != nil {
		log.Println("Open failed :", err)
		return
	}

	meta, err := fs.Write(filename, []byte(hello_lyrics))
	if err != nil {
		log.Println("Write Failed :", err)
		return
	}

	log.Printf("Write :meta %+v\n", meta)
	meta.Version = -1; //set it since we dont care about the version.

	err = fs.Write_meta(filename, meta)
	if err != nil {
		log.Println("Write_meta Failed :", err)
		return
	}

	err = fs.Read_lock(filename)
	if err != nil {
		log.Println("Read_lock Failed :", err)
		return
	}

	out, err := fs.Read_op(filename, []string{"grep -i hello ", " "})
	if err != nil {
		log.Println("Read_op failed: ", err)
		return
	}

	log.Println("Read_op ret:", out)

	err = fs.Read_unlock(filename)
	if err != nil {
		log.Println("Read_unlock Failed :", err)
		return
	}

	fs.Close()

}


func (c *Config) read_bench() {
	var err error
	filename := "client_read_bench"

	fs, err := nwfslib.Open(filename, c.Zk_servers, c.Back_servers,
			    c.Locking)
	if err != nil {
		log.Println("Open failed :", err)
		return
	}

	meta, err := fs.Write(filename, []byte(hello_lyrics))
	if err != nil {
		log.Println("Write Failed :", err)
		return
	}

	log.Printf("Write :meta %+v\n", meta)
	meta.Version = -1; //set it since we dont care about the version.

	err = fs.Write_meta(filename, meta)
	if err != nil {
		log.Println("Write_meta Failed :", err)
		return
	}

	start := time.Now()
	for i:=0; i<BENCH_LOOP_LEN; i++ {
		fmt.Printf(".")
		if i%50 == 49 {
			fmt.Println(i+1)
		}

		err = fs.Read_lock(filename)
		if err != nil {
			log.Println("Read_lock Failed :", err)
			return
		}

		_, err := fs.Read_op(filename, []string{"grep -i hello ", " "})
		if err != nil {
			log.Println("Read_op failed: ", err)
			return
		}

		err = fs.Read_unlock(filename)
		if err != nil {
			log.Println("Read_unlock Failed :", err)
			return
		}
	}
	elapsed := time.Now().Sub(start)
	log.Println("Lock-Read-Unlock:", elapsed, "/", BENCH_LOOP_LEN, " iters")


	start = time.Now()
	err = fs.Read_lock(filename)
	if err != nil {
		log.Println("Read_lock Failed :", err)
		return
	}
	for i:=0; i<BENCH_LOOP_LEN; i++ {
		fmt.Printf(".")
		if i%50 == 49 {
			fmt.Println(i+1)
		}

		_, err := fs.Read_op(filename, []string{"grep -i hello ", " "})
		if err != nil {
			log.Println("Read_op failed: ", err)
			return
		}
	}
	err = fs.Read_unlock(filename)
	if err != nil {
		log.Println("Read_unlock Failed :", err)
		return
	}
	elapsed = time.Now().Sub(start)
	log.Println("Reads:", elapsed, "/", BENCH_LOOP_LEN, " iters")

}

func (c *Config) rand_rw_bench() {
	var err error
	filename := "client_read_bench"

	fs, err := nwfslib.Open(filename, c.Zk_servers, c.Back_servers,
			    c.Locking)
	if err != nil {
		log.Println("Open failed :", err)
		return
	}
	last_read_ver := -1

	rand.Seed(time.Now().UnixNano())

	rw_arr := "w"
	writes := 1
	reads := 0
	for i:=0; i< BENCH_LOOP_LEN-1; i++ {
		toss := rand.Float32()
		if toss > c.Read_prob {
			rw_arr += "r"
			reads++
		} else {
			rw_arr += "w"
			writes++
		}
	}

	log.Println("Reads", reads, "Writes", writes, "arr", rw_arr, len(rw_arr));

	start := time.Now()
	for i, op := range rw_arr {
		if op == 'w' {
			meta, err := fs.Write(filename, []byte(hello_lyrics))
			if err != nil {
				log.Println("Write Failed :", err)
				return
			}

			meta.Version = int32(last_read_ver);
			//set it since we dont care about the version.

			err = fs.Write_meta(filename, meta)
			if err != nil {
				log.Println("Write_meta Failed :", err)
				return
			}

		} else if op == 'r' {

			err = fs.Read_lock(filename)
			if err != nil {
				log.Println("Read_lock Failed :", err)
				return
			}

			fmt.Printf(".")
			if i%50 == 49 {
				fmt.Println(i+1)
			}

			_, err := fs.Read_op(filename,
						[]string{"grep -i hello ", " "})
			if err != nil {
				log.Println("Read_op failed: ", err)
				return
			}
			//last_read_ver = meta.Version

			err = fs.Read_unlock(filename)
			if err != nil {
				log.Println("Read_lock Failed :", err)
				return
			}
		}
	}
	elapsed := time.Now().Sub(start)
	log.Println("Random RW:", elapsed, "/", BENCH_LOOP_LEN, " iters")

}
