package main

import (
	"encoding/json"
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
	res ,_ := job.Pop()
	da := make(map[string]interface{})

	json.Unmarshal(res, &da)
	fmt.Println(da)

}

