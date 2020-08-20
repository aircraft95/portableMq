package mq

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/mediocregopher/radix/v3"
	"gomq/redis"
	"gopkg.in/mgo.v2/bson"
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

type Message struct {
	Id   string
	Data interface{}
}

func NewQueue(jobName string, i int64) *Queue {
	key := fmt.Sprintf(":list-%d", i)
	queue := &Queue{Name: jobName+key}
	return queue
}

func (queue *Queue) push (message Message) (err error)  {
	dataByte ,err := json.Marshal(message)

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

func (queue *Queue) receive() (message Message , err error)  {
	con := redis.GetPool()
	var val []string
	err = con.Do(radix.Cmd(&val, "BLPOP", queue.Name, "10"))
	if err != nil {
		return
	}
	if len(val) == 2 {
		value := []byte(val[1])
		json.Unmarshal(value, &message)
	} else {
		err = errors.New("no message")
	}
	return
}

func (queue *Queue) DeleteMessage(value []byte)   {

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
	message := Message{
		Id: bson.NewObjectId().Hex() ,
		Data: data,
	}
	err = queue.push(message)
	return
}


func (job *Job) Handle(handleFunc func(message Message) bool) (data []byte, err error) {
	var i int64
	for i = 0 ; i < job.Num; i++ {
		index := i
		go func() {
			for {
				message, err := job.List[index].receive()
				if err != nil {
					fmt.Println(err)
					continue
				}
				handleFunc(message)
			}
		}()

	}
	return
}

func RangeRand(min, max int64) int64 {
	if min > max {
		panic("the min is greater than max!")
	}

	result, _ := rand.Int(rand.Reader, big.NewInt(max-min+1))
	return min + result.Int64()
}

