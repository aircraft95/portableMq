package mq

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/mediocregopher/radix/v3"
	"gomq/redis"
	"math/big"
)

type Job struct {
	Name string
	Num           int64
	List   []*Queue
}

func NewJob(name string, Num int64) *Job {
	job := new(Job)
	job.Name = fmt.Sprintf("mq:job:name:%v", name)
	job.Num = Num
	job.initQueueList()
	return job
}

type Queue struct {
	Name   string
}

func NewQueue(jobName string, i int64) *Queue {
	key := fmt.Sprintf(":list-%d", i)
	queue := &Queue{Name: jobName+key}
	return queue
}

func (queue *Queue) push (data interface{}) (err error)  {
	dataByte ,err := json.Marshal(data)

	if err != nil {
		return
	}

	if len(dataByte) == 0{
		err = errors.New("data is nil")
		return
	}
	con := redis.GetPool()
	var ok bool
	err = con.Do(radix.Cmd(&ok, "RPUSH", queue.Name, string(dataByte)))
	return
}

func (queue *Queue) pop() (value []byte,err error)  {
	con := redis.GetPool()
	var val []string
	err = con.Do(radix.Cmd(&val, "BLPOP", queue.Name, "10"))
	if err != nil {
		return
	}
	if len(val) == 2 {
		value = []byte(val[1])
	}
	return
}


func (job *Job) initQueueList() {
	job.List = make([]*Queue, job.Num)
	var i int64
	for i = 0; i < job.Num; i++ {
		job.List[i] = NewQueue(job.Name, i)
	}
}


func (job *Job) getList() *Queue {
	//随机分配到某个list
	key := RangeRand(0, job.Num - 1)
	return job.List[key]
}

func (job *Job) Push(data interface{}) (err error) {
	queue := job.getList()
	err = queue.push(data)
	return
}

func (job *Job) Pop() (data []byte, err error) {
	queue := job.getList()
	data, err = queue.pop()
	return
}

func RangeRand(min, max int64) int64 {
	if min > max {
		panic("the min is greater than max!")
	}

	result, _ := rand.Int(rand.Reader, big.NewInt(max-min+1))
	return min + result.Int64()
}

