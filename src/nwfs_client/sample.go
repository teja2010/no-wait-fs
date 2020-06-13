package main

import (
	"log"
	"os"
	"strings"
	"bufio"
	"io/ioutil"
	"nwfslib"
	go_zk "github.com/samuel/go-zookeeper/zk"
)

const (
	text_file = "large_text_file"
)

func (c *Config) Push_data(filepath, name string) {

	td, err := os.Open(filepath)
	if err != nil {
		log.Println("Open error", err)
		return
	}
	defer td.Close()

	text_data, err := ioutil.ReadAll(td)
	if err != nil {
		log.Println("Readall error", err)
		return
	}

	fs, err := nwfslib.Open(name, c.Zk_servers, c.Back_servers,
	c.Locking)
	if err != nil {
		log.Println("Open failed", err)
		return
	}
	defer fs.Close()

	meta, err := fs.Write(name, []byte(text_data))
	if err != nil {
		log.Println("Write failed", err)
		return
	}

	meta.Version = -1
	err = fs.Write_meta(name, meta)
	if err != nil {
		log.Println("Write meta failed",err)
		return
	}
}


func (c *Config) App_Text_Processing(tid int, ch chan int64) {

	if tid > c.Threads_num {
		log.Println("TID too large")
		ch<- (-1)
		return
	}


	wordmap := c.getWords(tid)

	fs, err := nwfslib.Open(text_file, c.Zk_servers, c.Back_servers,
			    c.Locking)
	if err != nil {
		log.Println("Open failed", err)
		ch<- (-1)
		return
	}
	defer fs.Close()

	// check if a word is found in the file.

	corrections := int64(0)
	conflicts := int64(0)
	ver := int32(-1)
	for len(wordmap) > 0 { //stuck here till wordmap is empty
	for wrong_word := range wordmap {
		err = fs.Read_lock(text_file)
		if err != nil {
			log.Println("Read_lock failed :", err)
			ch<- (-1)
			return
		}

		var grep_out string
		ver, grep_out, err = fs.Read_op(text_file,
					[]string{"grep -i "+wrong_word+" ", " "})
		if err != nil {
			log.Println("Read_op failed: ", err)
			ch<- (-1)
			return
		}

		//log.Printf("<%s>\n",grep_out)

		var meta *nwfslib.Metadata
		if len(grep_out) > 0 {
			sed_cmd := "sed s/"+wrong_word+"/"+
						wordmap[wrong_word] + "/g "
			ver, meta, err = fs.Write_op(text_file,
						     []string{sed_cmd, " "})
			if err != nil {
				log.Println("Write_op failed")
				return
			}
			meta.Version = ver
		}

		err = fs.Read_unlock(text_file)
		if err != nil {
			log.Println("Read_lock failed :", err)
			ch<- (-1)
			return
		}

		if len(grep_out) == 0 {
			log.Println("Did not find", wrong_word)
			delete(wordmap, wrong_word)
			continue
		}

		err = fs.Write_meta(text_file, meta)
		if err == go_zk.ErrBadVersion {
			log.Println(err)
			conflicts++
			continue
		} else if err != nil {
			log.Println("Write_meta failed", err)
			ch<- (-1)
			return
		}

		log.Println("Corrected ", wrong_word)
		delete(wordmap, wrong_word)
		corrections++
	}
	}

	ch <- ((corrections << 32) & conflicts)
}

func (c *Config) getWords(tid int) map[string]string {

	wordmap := make(map[string]string)

	wfile, err := os.Open(c.Wrong_wordlist_file)
	if err != nil {
		log.Println("cant open", c.Wrong_wordlist_file,", err: ", err)
		return nil
	}

	sc:= bufio.NewScanner(wfile)
	sc.Split(bufio.ScanLines)
	count := 0
	for sc.Scan() {
		count++
	}
	start := tid*(count/c.Threads_num)
	end := (tid + 1)*(count/c.Threads_num)
	//log.Println(count, start, end)

	wfile.Close()
	wfile2, err := os.Open(c.Wrong_wordlist_file)
	if err != nil {
		log.Println("cant open", c.Wrong_wordlist_file,", err: ", err)
		return nil
	}
	defer wfile2.Close()

	scanner := bufio.NewScanner(wfile2)
	scanner.Split(bufio.ScanLines)

	count = -1
	for scanner.Scan() {
		count++
		line := scanner.Text()
		if count <= start || count > end {
			continue
		}
		idx := strings.Index(line, ", ")
		if idx < 0 {
			log.Println("Error parsing", line)
			continue
		}

		first := line[:idx]
		second := line[idx+2:]
		wordmap[first] = second
	}

	return wordmap
}

func (c *Config) App_Syslog() {
	log.Println("App_Syslog")
}
