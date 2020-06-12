package main

import (
	"log"
	"fmt"
	"os"
	//"io"
	"time"
	"strconv"
	"os/exec"
	//"math"
	"math/rand"
	"io/ioutil"
	"encoding/json"
	"nwfslib"
	go_zk "github.com/samuel/go-zookeeper/zk"
)

const (
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
	Read_prob float32
	Threads_num int
	Locking string
	BENCH_LOOP_LEN int
	Sys_op_arr []string
	Ignore_Version bool

	Push_sys_out bool

	Read_ops [][]string

	Test_backend bool
	Test_hello bool
	Test_benchmark_reads bool
	Test_backend_bench bool
	Test_rand_rw_bench bool
	Test_sys_logs bool

	Push_sample_data bool
	Text_file string

	Sample_Text_Processing bool
	Wrong_wordlist_file string

	Sample_Fill_CSV bool
	Sample_Syslog bool


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

func main() {
	args := os.Args[1:]

	if len(args) != 1 {
		print_help()
	}
	rand.Seed(time.Now().UnixNano())

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
		res := make(chan int64, config.Threads_num)

		for i:=0; i< config.Threads_num; i++ {
			go config.rand_rw_bench(res)
		}

		var avg int64
		count := int64(0)
		for i:=0; i< config.Threads_num; i++ {
			temp := <-res
			if temp <= 0 {
				continue
			}
			avg += temp
			count++
		}
		avg = avg/count
		avg_time, err := time.ParseDuration(strconv.Itoa(int(avg))+"us")
		if err != nil {
			log.Println("time.ParseDuration err:", err)
		} else {
			log.Println("Random RW: ", avg_time, "/",
					config.BENCH_LOOP_LEN, " iters,",
					count, "threads")
		}
	}
	if config.Test_sys_logs {
		config.sys_log_bench()
	}

	if config.Push_sample_data {
		config.Push_data(config.Text_file, text_file)
		return
	}


	if config.Sample_Text_Processing {
		config.App_Text_Processing()
	}
	if config.Sample_Fill_CSV {
		config.App_Fill_CSV()
	}
	if config.Sample_Syslog {
		config.App_Syslog()
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
	defer nrc.Close()

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
	filename := "clientdir_backend_bench"

	var meta *nwfslib.Metadata
	start := time.Now()
	for i:=0; i<c.BENCH_LOOP_LEN; i++ {
		fmt.Printf(".")
		if i%50 == 49 {
			fmt.Println(i+1)
		}

		var err error
		meta, err = nwfslib.Write_shards(filename,
					[]byte(c.get_system_stats()),
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
	log.Println("Backend Write Bench", elapsed, "/", c.BENCH_LOOP_LEN, " iters")

	meta_bytes, err := json.Marshal(meta)
	if err != nil {
		log.Println("json.Marshal failed: ", err)
		return
	}

	start = time.Now()
	for i:=0; i<c.BENCH_LOOP_LEN; i++ {
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
	log.Println("Backend Read Bench", elapsed, "/", c.BENCH_LOOP_LEN, " iters")


}

func (c *Config) hello_test() {
	var err error
	filename := "clientdir_hello_test"

	fs, err := nwfslib.Open(filename, c.Zk_servers, c.Back_servers,
			    c.Locking)
	if err != nil {
		log.Println("Open failed :", err)
		return
	}
	defer fs.Close()

	meta, err := fs.Write(filename, []byte(hello_lyrics+"\n"+hello_lyrics))
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

	_, out, err := fs.Read_op(filename, []string{"grep -i hello ", " "})
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
	defer fs.Close()

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
	for i:=0; i<c.BENCH_LOOP_LEN; i++ {
		fmt.Printf(".")
		if i%50 == 49 {
			fmt.Println(i+1)
		}

		err = fs.Read_lock(filename)
		if err != nil {
			log.Println("Read_lock Failed :", err)
			return
		}

		_, _, err := fs.Read_op(filename, []string{"grep -i hello ", " "})
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
	log.Println("Lock-Read-Unlock:", elapsed, "/", c.BENCH_LOOP_LEN, " iters")


	start = time.Now()
	err = fs.Read_lock(filename)
	if err != nil {
		log.Println("Read_lock Failed :", err)
		return
	}
	for i:=0; i<c.BENCH_LOOP_LEN; i++ {
		fmt.Printf(".")
		if i%50 == 49 {
			fmt.Println(i+1)
		}

		_, _, err := fs.Read_op(filename, []string{"grep -i hello ", " "})
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
	log.Println("Reads:", elapsed, "/", c.BENCH_LOOP_LEN, " iters")

}

func (c *Config) rand_rw_bench(result chan int64) {
	result <- c._rand_rw_bench()
}
func (c *Config) _rand_rw_bench() int64 {
	var err error
	filename := "client_rw_bench_" + c.Locking

	fs, err := nwfslib.Open(filename, c.Zk_servers, c.Back_servers,
			    c.Locking)
	if err != nil {
		log.Println("Open failed :", err)
		return -1
	}
	defer fs.Close()
	last_read_ver := int32(-1)
	lock_again_count := 0

	rw_arr := "w"
	writes := 1
	reads := 0
	for i:=0; i< c.BENCH_LOOP_LEN-1; i++ {
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
		fmt.Printf("%c",op)
		if i%50 == 49 {
			fmt.Println(i+1)
		}

		if op == 'w' {
			meta, err := fs.Write(filename, []byte(hello_lyrics))
			if err != nil {
				log.Println("Write Failed :", err)
				return -1
			}

			if c.Ignore_Version {
				meta.Version = -1
			} else {
				meta.Version = int32(last_read_ver);
			}

			//set it since we dont care about the version.

			err = fs.Write_meta(filename, meta)
			if err == go_zk.ErrBadVersion {
				fmt.Printf("V")
				continue
			}
			if err != nil {
				log.Println("Write_meta Failed :", err)
				return -1
			}

		} else if op == 'r' {

			if ((i > 0 && rw_arr[i-1] != 'r') ||
			    (i == 0) || (lock_again_count == 0)) {
				err = fs.Read_lock(filename)
				if err != nil {
					log.Println("Read_lock Failed :", err)
					return -1
				}
				lock_again_count++
			}


			ver, _, err := fs.Read_op(filename,
						[]string{"grep -i hello ", " "})
			if err != nil {
				log.Println("Read_op failed: ", err)
				return -1
			}
			last_read_ver = ver
			fmt.Printf("%d",ver)

			if ((i+1 < c.BENCH_LOOP_LEN && rw_arr[i+1] != 'r') ||
			    (i == c.BENCH_LOOP_LEN) ||
			    (lock_again_count >= 5)) {
				err = fs.Read_unlock(filename)
				if err != nil {
					log.Println("Read_lock Failed :", err)
					return -1
				}
				lock_again_count = 0
			}
		}
	}
	elapsed := time.Now().Sub(start)
	//log.Println("Random RW:", elapsed, "/", c.BENCH_LOOP_LEN, " iters")
	return elapsed.Microseconds()
}

func (c* Config) get_system_stats() string {

	data := ""
	for _, op := range c.Sys_op_arr {
		cmd := exec.Command("sh", "-c", op)
		out, err := cmd.CombinedOutput()
		if err != nil {
			log.Println("system_stats", err, "|", out)
		} else {
			data += string(out)
		}
	}

	return data
}

// TODO: complete the function
func (c *Config) sys_log_bench() {

	last_read_ver := int32(-1)
	for {
		log.Println("--- Wait for", c.Periodic_ms, "Milliseconds ---")
		time.Sleep(time.Duration(c.Periodic_ms)*time.Millisecond)

		var err error
		filename := "client_rw_bench"

		fs, err := nwfslib.Open(filename, c.Zk_servers, c.Back_servers,
				    c.Locking)
		if err != nil {
			log.Println("Open failed :", err)
			return
		}
		defer fs.Close()

		if c.Push_sys_out {
			sys_out := c.get_system_stats()

			meta, err := fs.Write(filename, []byte(sys_out))
			if err != nil {
				log.Println("Write Failed :", err)
				return
			}

			if c.Ignore_Version {
				meta.Version = -1
			} else {
				meta.Version = last_read_ver
			}
			//set it since we dont care about the version.

			err = fs.Write_meta(filename, meta)
			if err != nil {
				log.Println("Write_meta Failed :", err)
				return
			}
		} else { //keep reading
		}
	}
}
