package main

import (
	"fmt"
	"gomq/mq"
	"gomq/redis"
	"net/http"
	_ "net/http/pprof"
	"time"
)

func main() {
	job := mq.NewJob("test", "d:/tmp/fail-queue.json", 1, redis.GetPool(), func(message mq.Message) bool {
		data := message.Data.(map[string]interface{})
		fmt.Println(data)
		fmt.Println(data["age"].(float64))
		return true
	})

	data := map[string]interface{}{
		"name": "hello",
		"age":  18,
	}
	_ = job.Push(data)

	go func() {
		time.Sleep(time.Second * 10)
		data := map[string]interface{}{
			"name": "mmm",
		}
		_ = job.Push(data)
	}()

	http.ListenAndServe("localhost:6060", nil)

}
