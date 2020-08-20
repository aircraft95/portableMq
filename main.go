package main

import (
	"fmt"
	"gomq/mq"
)

func main()  {
	job := mq.NewJob("test",1)
	data := map[string]interface{}{
		"name" : "hello",
		"age" : 18,
	}
	job.Push(data)

	_, _ = job.Handle(func(message mq.Message) bool {
		data := message.Data
		fmt.Println(data)
		return true
	})

	for {
		select {

		}
	}
}

