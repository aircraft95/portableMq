package main

import (
	"gomq/mq"
	"gomq/redis"
	"net/http"
	_ "net/http/pprof"
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

	http.ListenAndServe("localhost:6060", nil)

	//
	//for {
	//	select {
	//
	//	}
	//}





}

