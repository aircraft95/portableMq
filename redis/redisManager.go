package redis

import (
	"github.com/mediocregopher/radix/v3"
	"time"
)

var redisPool *radix.Pool

func init() {
	connFunc := func(network, addr string) (radix.Conn, error) {
		return radix.Dial(network, addr,
			radix.DialTimeout(1*time.Minute),
		)
	}
	redisPool, _ = radix.NewPool("tcp", ":6379", 10, radix.PoolConnFunc(connFunc))
}

func GetPool() *radix.Pool {
	return redisPool
}