//golang+redis 简单版mq ,有ack功能
package portableMq

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
	//队列名称
	name string
	//开启list数量
	num int64
	//消息被POP后,等待ack的doing表的redis key名 在redis是有序集合
	doingTable string
	//开启的list
	queue []*queue
	//redis 连接池指针
	redisConn *radix.Pool
	//callback处理消息的函数
	handler func(message Message) bool
	wg      sync.WaitGroup
	persistent
}

type persistent struct {
	path string //redis连接失效,临时存放message的文件地址
	mu   sync.RWMutex
}

//工作池
type jobPool struct {
	job    map[string]*job
	lock   sync.RWMutex
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

var JobPool *jobPool

func getJobPool() *jobPool {
	if JobPool == nil {
		ctx, cancel := context.WithCancel(context.Background())
		JobPool = &jobPool{
			job:    make(map[string]*job),
			ctx:    ctx,
			cancel: cancel,
		}
		go JobPool.closeHandler()
	}
	return JobPool
}

func (j *jobPool) closeHandler() {
	for {
		select {
		//当job接受到退出信号后,最先退出的job后会发送jobPool池准备退出的信息
		case <-j.ctx.Done():
			j.wg.Wait() //等待所有job退出完成
			fmt.Println("exit success")
			os.Exit(0)
		}
	}
}

type handlerFn func(message Message) bool

// example:
//
//job := mq.NewJob("test", "/fail-queue.json", 1, redis.GetPool(), func(message mq.Message) bool {
//	data := message.Data
//	fmt.Println(data)
//	return true
//})
//
func NewJob(name, persistentPath string, num int64, conn *radix.Pool, handler handlerFn) *job {
	name = fmt.Sprintf("mq:job:name:%v", name)
	jobPool := getJobPool()
	jobPool.lock.RLock()
	if job, ok := jobPool.job[name]; ok {
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
		name:       name,
		num:        num,
		doingTable: name + ":doing",
		redisConn:  conn,
		handler:    handler,
		persistent: persistent{
			path: persistentPath,
		},
	}

	//注册一个退出信号
	ctx, cancel := context.WithCancel(context.Background())
	//初始化工作队列
	newJob.initQueueList()

	//开启redis的消息回滚处理
	go newJob.rollbackDoingRedisMsg(ctx)
	//开启file的消息回滚处理(防止redis挂掉的应急方案)
	go newJob.rollbackDoingFileMsg(ctx)
	//启动处理中心
	go newJob.handleCenterRun(ctx)

	//监听退出信号
	newJob.initSignalHandler(cancel)

	jobPool.wg.Add(1)
	jobPool.job[name] = newJob
	jobPool.lock.Unlock()
	return newJob
}

func GetJob(name string) (*job, error) {
	name = fmt.Sprintf("mq:job:name:%v", name)
	jobPool := getJobPool()
	jobPool.lock.RLock()
	defer jobPool.lock.RUnlock()
	if job, ok := jobPool.job[name]; ok {
		return job, nil
	} else {
		return nil, errors.New("job not exists")
	}
}

func (j *job) initQueueList() {
	j.queue = make([]*queue, j.num)
	var i int64
	for i = 0; i < j.num; i++ {
		j.queue[i] = newQueue(j.name, i, j.redisConn)
	}
}

//负责回滚redis消息的函数，采用有序集合，分数使用时间戳，获取时间戳是0到目前的时间范围的message,获取到的消息就是需要返回给队列的数据
func (j *job) rollbackDoingRedisMsg(ctx context.Context) {
	con := j.redisConn
	j.wg.Add(1)
	for {
		select {
		//收到退出信号处理
		case <-ctx.Done():
			j.wg.Done()
			return
		default:
			var value []string
			var message Message
			expireTime := strconv.FormatInt(time.Now().Unix(), 10)
			//取出有序集合中所有超时消息
			err := con.Do(radix.FlatCmd(&value, "ZRANGEBYSCORE", j.doingTable, "0", expireTime))
			if err != nil || len(value) == 0 {
				time.Sleep(time.Second * 1)
				continue
			}
			for _, v := range value {
				_ = json.Unmarshal([]byte(v), &message)
				err = j.getQueue().push(message)
				if err == nil {
					err = con.Do(radix.Cmd(&value, "ZREM", j.doingTable, v))
				}
				fmt.Println("rollback:", v)
			}
			time.Sleep(time.Second * 1)
		}
	}
}

func (j *job) getQueue() *queue {
	//随机分配到某个queue
	key := rangeRand(0, j.num-1)
	return j.queue[key]
}

// example:
//
//	job := mq.NewJob("test", "/fail-queue.json", 1, redis.GetPool(), func(message mq.Message) bool {
//		data := message.Data
//		fmt.Println(data)
//		return true
//	})
//	data := map[string]interface{}{
//		"name": "mike",
//		"age":  18,
//	}
//	_ = job.Push(data)
//
func (j *job) Push(data interface{}) (err error) {
	queue := j.getQueue()
	message := Message{
		Id:   bson.NewObjectId().Hex(),
		Data: data,
	}
	err = queue.push(message)
	return
}

// example:
//
//	job := mq.NewJob("test", "/fail-queue.json", 1, redis.GetPool(), func(message mq.Message) bool {
//		data := message.Data
//		fmt.Println(data)
//		return true
//	})
//	mike := map[string]interface{}{
//		"name": "mike",
//		"age":  18,
//	}
//
//	john := map[string]interface{}{
//		"name": "john",
//		"age":  20,
//	}
//
//	data := []interface{}{
//		mike,
//		john,
//	}
//	_ = job.BatchPush(data)
//
func (j *job) BatchPush(data []interface{}) (err error) {
	queue := j.getQueue()
	var messages []Message
	for _, v := range data {
		message := Message{
			Id:   bson.NewObjectId().Hex(),
			Data: v,
		}
		messages = append(messages, message)
	}

	err = queue.batchPush(messages)
	return
}

func (j *job) handleCenterRun(ctx context.Context) {
	var i int64
	for i = 0; i < j.num; i++ {
		index := i
		j.wg.Add(1)
		go func(ctx context.Context, index int64) {
			j.handleCallback(ctx, index)
		}(ctx, index)
	}
}

func (j *job) handleCallback(ctx context.Context, index int64) {
	//拦截错误,重启处理函数
	defer func(ctx context.Context, index int64) {
		if r := recover(); r != nil {
			fmt.Printf("handle have been err : %v \r\n", r)
			fmt.Printf("restart job %v handle goroutine index: %v \r\n", j.name, index)
			j.handleCallback(ctx, index)
		}
	}(ctx, index)
	queue := j.queue[index]
	for {
		select {
		case <-ctx.Done():
			j.wg.Done()
			return
		default:
			message, err := queue.receiveMessage(j)
			if err != nil {
				continue
			}
			if j.handler(message) {
				err = queue.deleteMessage(message)
				if err != nil {
					fmt.Println(err)
				}
			}
		}
	}
}

func (j *job) writeFileQueueJob(message Message) {
	jsonByte, _ := json.Marshal(message)
	file, err := os.OpenFile(j.persistent.path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0755)
	if err != nil {
		return
	}
	_, _ = file.WriteString(string(jsonByte) + "\r\n")
}

func (j *job) readFileQueueJob() (message Message, err error) {
	j.persistent.mu.Lock()
	defer j.persistent.mu.Unlock()
	f, err := os.OpenFile(j.persistent.path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return
	}
	line, err := popLine(f)
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

//负责回滚消息,处理被临时存到文件中的message
func (j *job) rollbackDoingFileMsg(ctx context.Context) {
	f, err := os.OpenFile(j.persistent.path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return
	}
	//如果文件无法读写默认停用磁盘临时保存的方法
	_, err = f.Stat()
	if err != nil {
		return
	}
	j.wg.Add(1)

	for {
		select {
		case <-ctx.Done():
			j.wg.Done()
			return
		default:
			message, err := j.readFileQueueJob()
			if err != nil {
				time.Sleep(time.Second * 1)
				continue
			}
			err = j.getQueue().push(message)
			//如果还是无法插入到redis的队列中，就重新写到文件
			if err != nil {
				j.writeFileQueueJob(message)
				time.Sleep(time.Second * 1)
				continue
			}
			fmt.Println("file rollback:", message)
			time.Sleep(time.Second * 1)
		}
	}
}

func (j *job) initSignalHandler(cancel context.CancelFunc) {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	go func() {
		<-sig
		cancel()          // 通知各个服务退出
		j.wg.Wait()       //等待退出完成
		JobPool.cancel()  //通知job工作池开启退出工作
		JobPool.wg.Done() //消耗掉该job占用的wg
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
	name       string
	doingTable string
	redisConn  *radix.Pool
}

type Message struct {
	Id   string
	Data interface{}
}

func newQueue(jobName string, i int64, redisConn *radix.Pool) *queue {
	name := fmt.Sprintf("%s:queue-%d", jobName, i)
	queue := &queue{
		name:       name,
		doingTable: jobName + ":doing",
		redisConn:  redisConn,
	}
	return queue
}

func (q *queue) push(message Message) (err error) {
	dataByte, err := json.Marshal(message)

	if err != nil {
		return
	}

	if len(dataByte) == 0 {
		err = errors.New("data is nil")
		return
	}
	con := q.redisConn
	var ok bool
	err = con.Do(radix.Cmd(&ok, "LPUSH", q.name, string(dataByte)))
	return
}

func (q *queue) batchPush(messages []Message) (err error) {
	if len(messages) == 0 {
		err = errors.New("data is nil")
		return
	}
	addArgs := []string{q.name}
	for _, message := range messages {
		dataByte, _ := json.Marshal(message)
		addArgs = append(addArgs, string(dataByte))
	}

	con := q.redisConn
	var ok bool
	err = con.Do(radix.Cmd(&ok, "LPUSH", addArgs...))
	return
}

func (q *queue) receiveMessage(job *job) (message Message, err error) {
	con := q.redisConn
	var val []string
	err = con.Do(radix.Cmd(&val, "BRPOP", q.name, "10"))
	if err != nil {
		return
	}
	if len(val) == 2 {
		value := []byte(val[1])
		json.Unmarshal(value, &message)
		err = q.addDoing(message)
		if err != nil {
			for k := 0; k < 4; k++ {
				err = q.push(message)
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

func (q *queue) addDoing(message Message) (err error) {
	con := q.redisConn
	dataByte, err := json.Marshal(message)
	if err != nil {
		return
	}
	expireTime := strconv.FormatInt(time.Now().Unix()+60, 10)
	var ok bool
	err = con.Do(radix.Cmd(&ok, "ZADD", q.doingTable, expireTime, string(dataByte)))

	if !ok {
		err = errors.New("add redis doing table fail")
	}
	return
}

func (q *queue) deleteMessage(message Message) (err error) {
	con := q.redisConn
	dataByte, err := json.Marshal(message)
	if err != nil {
		return
	}
	var ok bool
	err = con.Do(radix.Cmd(&ok, "ZREM", q.doingTable, string(dataByte)))

	if !ok {
		err = errors.New("delete message fail")
	}
	return
}

//随机函数
func rangeRand(min, max int64) int64 {
	if min > max {
		panic("the min is greater than max!")
	}

	result, _ := rand.Int(rand.Reader, big.NewInt(max-min+1))
	return min + result.Int64()
}
