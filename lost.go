package main

import (
	"./protocol"
	"bytes"
	"flag"
	"fmt"
	"github.com/golang/protobuf/proto"
	"gopkg.in/redis.v5"
	"log"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	read_client *redis.Client
	send_client *redis.Client
	running     int32

	quit  sync.WaitGroup
	pushn int64
	popn  int64
)

const (
	SEND_KEY = "list1"
	READ_KEY = "list1"
)

func read() {
	for running == 1 {
		result, err := read_client.BLPop(1*time.Second, READ_KEY).Result()
		if err != nil {
			log.Printf("redis BLPop err. %s === %#v", err, err)
			if err == redis.Nil {
				continue
			}
			if neterr, ok := err.(*net.OpError); ok {
				if neterr.Timeout() {
					continue
				}
			}
			continue
		}
		atomic.AddInt64(&popn, 1)
		//if result[0] != READ_KEY {
		//	continue
		//}
		mproto := &protocol.MProtocolHeader{}
		err = proto.Unmarshal([]byte(result[1]), mproto)
		if err != nil {
			log.Printf("redis unmarshal err. %s", err)
			continue
		}
		//log.Printf("redis rcv: MsgID: %s", mproto.MsgID)
	}
	quit.Done()
}

var sleep = flag.Int("n", 5000, "")
var host = flag.String("h", "100.69.198.63:6379", "host")
var host2 = flag.String("h2", "100.69.198.63:6379", "host")
var host3 = flag.String("h3", "100.69.198.63:6379", "host")
var pwd = flag.String("p", "rds", "password")
var db = flag.Int("d", 14, "db")
var master = flag.String("m", "", "master")

func main() {
	flag.Parse()
	rand.Seed(time.Now().Unix())
	if *master == "" {
		send_client = redis.NewClient(&redis.Options{
			Addr:     *host,
			Password: *pwd,
			DB:       *db,
			//PoolSize:  pool,
		})
		read_client = redis.NewClient(&redis.Options{
			Addr:     *host,
			Password: *pwd,
			DB:       *db,
			//PoolSize:  pool,
		})
	} else {
		send_client = redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:    *master,
			SentinelAddrs: []string{*host},
			Password:      *pwd,
			DB:            *db,
		})
		read_client = redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:    *master,
			SentinelAddrs: []string{*host},
			Password:      *pwd,
			DB:            *db,
		})

	}
	quit.Add(1)
	running = 1
	go read()

	for i := 0; i < 2; i++ {

		quit.Add(1)
		go func() {
			var id uint
			for running == 1 {
				mproto := &protocol.MProtocolHeader{
					MsgID:   fmt.Sprintf("%d", id),
					To:      "sh",
					Payload: bytes.Repeat([]byte("a"), 1024),
				}
				id++
				data, _ := proto.Marshal(mproto)
				if err := PushRedis(data); err == nil {
					atomic.AddInt64(&pushn, 1)
				}
				time.Sleep(time.Duration(rand.Intn(*sleep)) * time.Millisecond)
			}
			quit.Done()
		}()
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM, os.Interrupt, os.Kill)

	// Block until a signal is received.
	<-c

	//time.Sleep(10000 * time.Millisecond)
	//buf := make([]byte, 100)
	//os.Stdin.Read(buf)
	running = 0
	quit.Wait()
	log.Printf("push: %d  pop: %d", pushn, popn)
}

func PushRedis(data []byte) error {
	_, err := send_client.RPush(SEND_KEY, data).Result()
	if err != nil {
		return err
	}
	//log.Printf("redis push %d", result)
	return nil
}

func FinitRedis() {
}
