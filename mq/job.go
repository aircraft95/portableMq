//golang+redis 简单版mq ,有ack功能
package mq

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/mediocregopher/radix/v3"
	"gopkg.in/mgo.v2/bson"
	"io"
	"math/big"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type job struct {
	name string
	num           int64
	doingTable  string
	list   []*queue
	redisConn  *radix.Pool
	handler  func(message Message) bool
	wg sync.WaitGroup
	persistent
}

type persistent struct {
	path string
	mu   sync.RWMutex
}

//工作池
type jobPool struct{
	job map[string]*job
	lock         sync.RWMutex
	wg sync.WaitGroup
	ctx context.Context
	cancel context.CancelFunc
}

var JobPool *jobPool

func getJobPool() *jobPool {
	if JobPool == nil {
		ctx, cancel := context.WithCancel(context.Background())
		JobPool = &jobPool{
			job:  make(map[string]*job),
			ctx : ctx,
			cancel : cancel,
		}
		go JobPool.closeHandler()
	}
	return JobPool
}

func (j *jobPool) closeHandler() {
	for {
		select{
		case <-j.ctx.Done():
			j.wg.Wait() //等待退出完成
			fmt.Println("退出完成")
			os.Exit(0)
		}
	}
}




type handlerFn func(message Message) bool

func NewJob(name, persistentPath string, num int64, conn *radix.Pool, handler handlerFn) *job {
	name = fmt.Sprintf("mq:job:name:%v", name)
	jobPool := getJobPool()
	jobPool.lock.RLock()
	if job,ok := jobPool.job[name]; ok {
		jobPool.lock.RUnlock()
		return job
	}
	jobPool.lock.RUnlock()

	if conn == nil {
		panic(errors.New("bad radix conn"))
	}

	if handler == nil {
		panic(errors.New("bad handler"))
	}

	jobPool.lock.Lock()
	newJob := &job{
		name : 			name,
		num : 			num,
		doingTable : 	name + ":doing",
		redisConn : 	conn,
		handler :		handler,
		persistent : 	persistent{
			path : 		persistentPath,
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	//初始化工作队列
	newJob.initQueueList()

	//开启redis ack
	go newJob.rollbackAck(ctx)
	//开启处理协程
	go newJob.handle(ctx)
	//开启持久化ack 用于取出数据后redis挂掉时的应急ack
	go newJob.ackFileMessage(ctx)

	newJob.initSignalHandler(cancel)

	jobPool.wg.Add(1)
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
func (job *job) rollbackAck (ctx context.Context) {
	con := job.redisConn
	job.wg.Add(1)
	for {
		select {
		case <-ctx.Done():
			job.wg.Done()
			return
		default:
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

func (job *job) handle(ctx context.Context) {
	if job.handler == nil {
		return
	}
	var i int64
	for i = 0 ; i < job.num; i++ {
		queue := job.list[i]
		job.wg.Add(1)
		go func(ctx context.Context) {
			for {
				select{
				case <-ctx.Done():
					job.wg.Done()
					return
				default:
					message, err := queue.receiveMessage(job)
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

			}
		}(ctx)
	}
}


func (job *job) writeFileQueueJob(message Message) {
	jsonByte, _ := json.Marshal(message)
	file, err := os.OpenFile(job.persistent.path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0755)
	if err != nil {
		return
	}
	_, _ = file.WriteString(string(jsonByte)+"\r\n")
}

func (job *job) readFileQueueJob() (message Message, err error) {
	job.persistent.mu.Lock()
	defer job.persistent.mu.Unlock()
	f, err := os.OpenFile(job.persistent.path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return
	}
	line ,err := popLine(f)
	if err != nil {
		return
	}
	if len(line) == 0 {
		err = errors.New("file is nil")
		return
	}

	lineStr := strings.Trim(string(line), "\r\n")
	json.Unmarshal([]byte(lineStr), &message)
	return
}

//ack被存到文件中的message
func (job *job) ackFileMessage(ctx context.Context) {
	f, err := os.OpenFile(job.persistent.path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return
	}
	//如果文件无法读写默认停用磁盘ack
	_, err = f.Stat()
	if err != nil {
		return
	}
	job.wg.Add(1)

	for {
		select {
		case <-ctx.Done():
			job.wg.Done()
			return
		default:
			message, err := job.readFileQueueJob()
			if err != nil {
				time.Sleep(time.Second * 1)
				continue
			}
			err = job.getList().push(message)
			//如果还是无法插入到redis的队列中，就重新写到文件
			if err != nil {
				job.writeFileQueueJob(message)
				time.Sleep(time.Second * 1)
				continue
			}
			fmt.Println("file ack:",message)
			time.Sleep(time.Second * 1)
		}
	}
}

func (job *job) initSignalHandler(cancel context.CancelFunc) {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	go func() {
		<-sig
		cancel() // 通知各个服务退出
		job.redisConn.Close()  //关闭redis连接
		job.wg.Wait() //等待退出完成
		fmt.Println("job退出完成,通知job连接池")
		JobPool.cancel()
		JobPool.wg.Done()
	}()
}

//删除文件第一行代码
func popLine(f *os.File) ([]byte, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	buf := bytes.NewBuffer(make([]byte, 0, fi.Size()))

	_, err = f.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}
	_, err = io.Copy(buf, f)
	if err != nil {
		return nil, err
	}

	line, err := buf.ReadBytes('\n')
	if err != nil && err != io.EOF {
		return nil, err
	}

	_, err = f.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}
	nw, err := io.Copy(f, buf)
	if err != nil {
		return nil, err
	}
	err = f.Truncate(nw)
	if err != nil {
		return nil, err
	}
	err = f.Sync()
	if err != nil {
		return nil, err
	}

	_, err = f.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}
	return line, nil
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

func (queue *queue) receiveMessage(job *job) (message Message , err error)  {
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
				} else {
					//最后一次还是无法插入，就写到文件中
					if k == 3 {
						job.writeFileQueueJob(message)
					}
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