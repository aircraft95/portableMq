package main

import (
	"fmt"
	"gomq/mq"
	"gomq/redis"
)

func main()  {
	mq.NewJob("test", "d:/tmp/fail-queue.json",1, redis.GetPool(), func(message mq.Message) bool {
		fmt.Println(message)
		return true
	})

	//data := map[string]interface{}{
	//	"name" : "hello",
	//	"age" : 18,
	//}
	//_ = job.Push(data)
	//
	//for {
	//	select {
	//
	//	}
	//}

	//message := mq.Message{
	//	Id:   bson.NewObjectId().Hex(),
	//	Data: data,
	//}
	//job.WriteQueueLob(message)




}

