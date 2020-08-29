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
		return true
	})

	mike := map[string]interface{}{
		"name": "mike",
		"age":  18,
	}

	john := map[string]interface{}{
		"name": "john",
		"age":  20,
	}

	data := []interface{}{
		mike,
		john,
	}
	_ = job.BatchPush(data)

	go func() {
		time.Sleep(time.Second * 10)
		data := map[string]interface{}{
			"name": "mmm",
		}
		_ = job.Push(data)
	}()

	http.ListenAndServe("localhost:6060", nil)

}
