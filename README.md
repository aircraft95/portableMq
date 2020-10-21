# portableMq

install：
```
go get -u github.com/aircraft95/portableMq
```

use：

```go
package main

import (
	"fmt"
	"github.com/aircraft95/portableMq"
	"github.com/mediocregopher/radix/v3"
	"time"
)


func main() {
	connFunc := func(network, addr string) (radix.Conn, error) {
		return radix.Dial(network, addr,
			radix.DialTimeout(1*time.Minute),
		)
	}
	redisPool, _ := radix.NewPool("tcp", ":6379", 10, radix.PoolConnFunc(connFunc))
	job := portableMq.NewJob("test", "/fail-queue.json", 1, redisPool, func(message portableMq.Message) bool {
		data := message.Data
		fmt.Println(data)
		return true
	})
	data := map[string]interface{}{
		"name": "mike",
		"age":  32,
	}
        //Immediate message
	_ = job.Push(data)
  
        //DelayPush message
	_ = job.DelayPush(data, 50)

	time.Sleep(time.Second * 100)

}
```
