package main

import (
	"fmt"
	"gomq/mq"
	"gomq/redis"
	"net/http"
	_ "net/http/pprof"
)

func main()  {
	job := mq.NewJob("test", "d:/tmp/fail-queue.json",100, redis.GetPool(), func(message mq.Message) bool {
		fmt.Println(message)
		return true
	})

	job1 := mq.NewJob("test1", "d:/tmp/fail-queue1.json",100, redis.GetPool(), func(message mq.Message) bool {
		fmt.Println(message)
		return true
	})

	job2 := mq.NewJob("test2", "d:/tmp/fail-queue2.json",100, redis.GetPool(), func(message mq.Message) bool {
		fmt.Println(message)
		return true
	})

	data := map[string]interface{}{
		"name" : "hello",
		"age" : 18,
	}
	_ = job.Push(data)


	_ = job2.Push(data)

	_ = job1.Push(data)

	http.ListenAndServe("localhost:6060", nil)


}

