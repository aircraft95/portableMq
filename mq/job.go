//golang+redis 简单版mq ,有ack功能
package mq

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/mediocregopher/radix/v3"
	"gopkg.in/mgo.v2/bson"
	"math/big"
	"strconv"
	"sync"
	"time"
)

type job struct {
	name string
	num           int64
	doingTable  string
	list   []*queue
	redisConn  *radix.Pool
	handler  func(message Message) bool
}

//工作池
var jobPool struct{
	job map[string]*job
	lock         sync.RWMutex
}

func NewJob(name string, Num int64, conn *radix.Pool, handler func(message Message) bool) *job {
	name = fmt.Sprintf("mq:job:name:%v", name)
	jobPool.lock.RLock()
	if job,ok := jobPool.job[name]; ok {
		jobPool.lock.RUnlock()
		//如果已经存在job 但是处理函数是空,但是传过来的处理函数非空,就重新装载处理函数
		if job.handler == nil && handler != nil {
			jobPool.lock.Lock()
			jobPool.job[name].handler = handler
			go jobPool.job[name].handle()
			jobPool.lock.Unlock()
		}
		return job
	}
	jobPool.lock.RUnlock()

	if conn == nil {
		panic(errors.New("bad radix conn"))
	}

	jobPool.lock.Lock()
	newJob := new(job)
	newJob.name = name
	newJob.num = Num
	newJob.doingTable = newJob.name + ":doing"
	newJob.redisConn = conn
	newJob.handler = handler
	newJob.initQueueList()

	go newJob.rollbackAck()
	go newJob.handle()

	if jobPool.job == nil {
		jobPool.job = make(map[string]*job)
	}
	jobPool.job[name] = newJob
	jobPool.lock.Unlock()
	return newJob
}



func (job *job) initQueueList() {
	job.list = make([]*queue, job.num)
	var i int64
	for i = 0; i < job.num; i++ {
		job.list[i] = newQueue(job.name, i, job.redisConn)
	}
}

//负责ack的函数，采用有序集合，分数使用时间戳，获取时间戳是0到目前的时间范围的message,获取到的消息就是需要返回给队列的数据
func (job *job) rollbackAck () {
	con := job.redisConn
	for {
		var value []string
		var message Message
		expireTime := strconv.FormatInt(time.Now().Unix(), 10)
		err := con.Do(radix.FlatCmd(&value, "ZRANGEBYSCORE", job.doingTable, "0", expireTime))
		if err != nil || len(value) == 0 {
			time.Sleep(time.Second * 1)
			continue
		}
		for _, v := range value {
			_ = json.Unmarshal([]byte(v), &message)
			err = job.getList().push(message)
			if err == nil {
				err = con.Do(radix.Cmd(&value, "ZREM", job.doingTable, v))
			}
			fmt.Println("ack:",v)
		}
	}
}

func (job *job) getList() *queue {
	//随机分配到某个list
	key := rangeRand(0, job.num - 1)
	return job.list[key]
}

func (job *job) Push(data interface{}) (err error) {
	queue := job.getList()
	message := Message{
		Id: bson.NewObjectId().Hex() ,
		Data: data,
	}
	err = queue.push(message)
	return
}

func (job *job) BatchPush(data []interface{}) (err error) {
	queue := job.getList()
	var messages []Message
	for _, v := range data {
		message := Message{
			Id: bson.NewObjectId().Hex() ,
			Data: v,
		}
		messages = append(messages, message)
	}

	err = queue.batchPush(messages)
	return
}

func (job *job) handle() {
	if job.handler == nil {
		return
	}
	var i int64
	for i = 0 ; i < job.num; i++ {
		queue := job.list[i]
		go func() {
			for {
				message, err := queue.receiveMessage()
				if err != nil {
					continue
				}
				if job.handler(message) {
					err = queue.deleteMessage(message)
					if err != nil {
						fmt.Println(err)
					}
				}
			}
		}()
	}
}


type queue struct {
	name   string
	doingTable  string
	redisConn *radix.Pool
}

type Message struct {
	Id   string
	Data interface{}
}

func newQueue(jobName string, i int64, redisConn *radix.Pool) *queue {
	key := fmt.Sprintf(":list-%d", i)
	queue := &queue{name: jobName+key, doingTable:jobName + ":doing", redisConn : redisConn}
	return queue
}

func (queue *queue) push (message Message) (err error)  {
	dataByte ,err := json.Marshal(message)

	if err != nil {
		return
	}

	if len(dataByte) == 0{
		err = errors.New("data is nil")
		return
	}
	con := queue.redisConn
	var ok bool
	err = con.Do(radix.Cmd(&ok, "LPUSH", queue.name, string(dataByte)))
	return
}

func (queue *queue) batchPush (messages []Message) (err error)  {
	if len(messages) == 0{
		err = errors.New("data is nil")
		return
	}
	addArgs := []string{queue.name}
	for _, message := range messages {
		dataByte ,_ := json.Marshal(message)
		addArgs = append(addArgs, string(dataByte))
	}

	con := queue.redisConn
	var ok bool
	err = con.Do(radix.Cmd(&ok, "LPUSH", addArgs...))
	return
}

func (queue *queue) receiveMessage() (message Message , err error)  {
	con := queue.redisConn
	var val []string
	err = con.Do(radix.Cmd(&val, "BRPOP", queue.name, "10"))
	if err != nil {
		return
	}
	if len(val) == 2 {
		value := []byte(val[1])
		json.Unmarshal(value, &message)
		err = queue.addAck(message)
		if err != nil {
			for k :=0 ; k < 4; k ++ {
				err = queue.push(message)
				if err == nil {
					break
				}
			}
		}
	} else {
		err = errors.New("no message")
	}
	return
}

func (queue *queue) addAck(message Message) (err error)  {
	con := queue.redisConn
	dataByte ,err := json.Marshal(message)
	if err != nil {
		return
	}
	expireTime := strconv.FormatInt(time.Now().Unix() + 60, 10)
	var ok bool
	err = con.Do(radix.Cmd(&ok, "ZADD", queue.doingTable, expireTime, string(dataByte)))

	if !ok {
		err = errors.New("add ack fail")
	}
	return
}


func (queue *queue) deleteMessage(message Message) (err error)   {
	con := queue.redisConn
	dataByte ,err := json.Marshal(message)
	if err != nil {
		return
	}
	var ok bool
	err = con.Do(radix.Cmd(&ok, "ZREM", queue.doingTable, string(dataByte)))

	if !ok {
		err = errors.New("delete message fail")
	}
	return
}




func rangeRand(min, max int64) int64 {
	if min > max {
		panic("the min is greater than max!")
	}

	result, _ := rand.Int(rand.Reader, big.NewInt(max-min+1))
	return min + result.Int64()
}
