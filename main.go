package main

import (
	"fmt"
	"gomq/mq"
	"gomq/redis"
)

func main()  {
	job := mq.NewJob("test", 1, redis.GetPool(), func(message mq.Message) bool {
		fmt.Println(message)
		return true
	})
	data := map[string]interface{}{
		"name" : "hello",
		"age" : 18,
	}
	_ = job.Push(data)

	for {
		select {

		}
	}
}

