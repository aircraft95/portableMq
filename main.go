package main

import (
	"gomq/mq"
	"gomq/redis"
)

func main()  {
	job := mq.NewJob("test", "d:/tmp/fail-queue.json",1, redis.GetPool(), nil)
	data := map[string]interface{}{
		"name" : "hello",
		"age" : 18,
	}
	_ = job.Push(data)


	_ = job.Push(data)

	_ = job.Push(data)

	//
	//for {
	//	select {
	//
	//	}
	//}





}

