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

func Exists(key string) bool {
	var ok int64
	e := redisPool.Do(radix.Cmd(&ok, "EXISTS", key))
	if e != nil || ok == 0 {
		return false
	}
	return true
}

func Expireat(key string, timestamp int64) error {
	return redisPool.Do(radix.FlatCmd(nil, "EXPIREAT", key, timestamp))
}

func Get(key string) string {
	var value string
	e := redisPool.Do(radix.Cmd(&value, "GET", key))
	if e != nil {
		return ""
	}
	return value
}

func Incr(key string) int {
	var value int
	e := redisPool.Do(radix.Cmd(&value, "INCR", key))
	if e != nil {
		return 0
	}
	return value
}

func Expire(key string, ttl int64) {
	redisPool.Do(radix.FlatCmd(nil, "EXPIRE", key, ttl))
}

func Delete(key string) error {
	return redisPool.Do(radix.Cmd(nil, "DEL", key))
}

func Set(key string, value string) error {
	return redisPool.Do(radix.FlatCmd(nil, "SET", key, value))
}

func Setex(key string, ttl int64, value string) {
	redisPool.Do(radix.FlatCmd(nil, "SETEX", key, ttl, value))
}

func Setnxex(key string, value string, ttl int64) string {
	var ok string
	e := redisPool.Do(radix.FlatCmd(&ok, "SET", key, value, "NX", "EX", ttl))
	if e != nil {
		return ""
	}
	return ok
}

func Ttl(key string) int64 {
	var ttl int64
	e := redisPool.Do(radix.Cmd(&ttl, "TTL", key))
	if e != nil {
		return -1
	}
	return ttl
}

func HLen(key string) int64 {
	var count int64
	e := redisPool.Do(radix.Cmd(&count, "HLEN", key))
	if e != nil {
		return 0
	}
	return count
}

func HExists(key string, hashKey string) bool {
	var ok int64
	e := redisPool.Do(radix.Cmd(&ok, "HEXISTS", key, hashKey))
	if e != nil || ok == 0 {
		return false
	}
	return true
}

func HSet(key string, hashKey string, value string) error {
	return redisPool.Do(radix.FlatCmd(nil, "HSET", key, hashKey, value))
}

func HGet(key string, hashKey string) string {
	var value string
	e := redisPool.Do(radix.Cmd(&value, "HGET", key, hashKey))
	if e != nil {
		return ""
	}
	return value
}

func IncrBy(key string, amount string) int {
	var value int
	e := redisPool.Do(radix.Cmd(&value, "INCRBY", key, amount))
	if e != nil {
		return 0
	}
	return value
}

func RPush(key string, value string) error {
	var ok int64
	return redisPool.Do(radix.Cmd(&ok, "RPUSH", key, value))
}

func LRem(key string, count int64, value string) error {
	var ok int64
	return redisPool.Do(radix.FlatCmd(&ok, "LREM", key, count, value))
}

func LPop(key string) interface{} {
	var value interface{}
	err := redisPool.Do(radix.Cmd(&value, "LPOP", key))
	if err != nil {
		return nil
	}
	return value
}
