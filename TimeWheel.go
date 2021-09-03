package main

import (
	"container/list"
	"errors"
	"fmt"
	"github.com/deckarep/golang-set"
	"math/rand"
	"reflect"
	"runtime"
	"strings"
	"time"
	"unicode"
)

// TaskData 回调函数参数类型
/*
时分秒都是 0-59 一共60个刻度
年 无上限先给个 10年刻度
月 1 - 12 12个刻度
日 1 - 31 不等
*/
type Roulette struct {
	name           string
	slots          []*list.List  // 时间轮槽
	slotNum        int64         // 槽数量
	currentPos     int64         // 当前指针指向哪一个槽
	isLastRoulette bool          // 最底层的时间轮盘,既最小刻度轮盘
	taskKeyMap     map[int64]int // 任务在第几个槽中保存，只保存存在的，查不到就默认不存在
	beforeRoulette *Roulette     // 上层的轮盘
	afterRoulette  *Roulette     // 下层轮盘
}

// Task 延时任务
type Task struct {
	delay        int64            // 延迟时间
	rouletteSite map[string]int64 // 在每个时间轮的位置
	key          int64            // 定时器唯一标识, 用于删除定时器
	Job          func()           //Job 延时任务回调函数
	crontab      *Crontab         // 重复调用时间表
}

// 是否是闰年
func leapYear(year int64) bool {
	//是否是闰年
	if year%100 == 0 {
		if year%400 == 0 {
			return true
		} else {
			return false
		}
	}
	if year%4 == 0 {
		return true
	} else {
		return false
	}
}

// 获取指定月份的天数
func getMonthDay(year, month int64) int64 {
	//month := r.beforeRoulette.currentPos
	if month == 4 || month == 6 || month == 9 || month == 11 {
		// 30天的月份 月份从1开始计数
		return 30
	}
	if month == 1 || month == 3 || month == 5 || month == 7 || month == 8 || month == 10 || month == 12 {
		// 31天的月份 月份从1开始计数
		return 31
	}
	//year := r.beforeRoulette.beforeRoulette.currentPos
	if leapYear(year) {
		return 29
	} else {
		return 28
	}
}

// 当前时间是否已经到达上限。
// 到达上限后需要在上一个时间刻度+1
func (r *Roulette) cycle() bool {
	/*
		年 不考虑
		月 12个月 最小数字 1 最大数字 12
		日 每个月天数不等 Roulette.getMonthDay 这个方法获取 具体天数
		时 24个刻度 最小数字 0 最大数字 23
		分 60个刻度 最小数字 0 最大数字 59
		秒 60个刻度 最小数字 0 最大数字 59
	*/
	if r.name == "month" {
		return r.currentPos == r.slotNum+1
	}
	if r.name == "day" {
		return r.currentPos == getMonthDay(r.beforeRoulette.beforeRoulette.currentPos, r.beforeRoulette.currentPos)+1
	}

	return r.currentPos == r.slotNum
}

// 主要函数入口
/*
定时调用该方法，从最底层的轮盘开始调用，、
相当于表盘中的秒针，每次都移动秒针，当秒针走完一圈就会带动分针走一格，依次类推
指针指向的格子中，所有的任务全部超时
*/
func (r *Roulette) tickHandler() {
	r.currentPos++
	if r.cycle() {
		// 刻度归零，先让上层的时间轮指针动起来，如果有分配task的情况，先分配到下层时间轮，这样如果有零点触发的任务就会执行了
		if r.name == "month" || r.name == "day" {
			r.currentPos = 1
		} else {
			r.currentPos = 0
		}

		if r.beforeRoulette != nil {
			r.beforeRoulette.tickHandler()
		}
	}
	tasks := r.slots[r.currentPos]
	if r.isLastRoulette {
		// 如果是最底层的时间轮的话就执行所有的任务
		if tasks != nil {
			r.runTask(*tasks)
			r.slots[r.currentPos] = list.New()
		}

	} else {
		// 不是执行任务的时间轮就向下层分配任务
		r.afterRoulette.assignTask(tasks)
	}

}

// 执行所有已到期的任务
func (r Roulette) runTask(taskList list.List) {
	for e := taskList.Front(); e != nil; e = e.Next() {
		task := e.Value.(*Task)
		fmt.Println("执行回调函数")
		go task.Job()
	}
}

// 判断任务是否需要立即执行，不需要的话就在下层轮盘中放置任务
func (r *Roulette) assignTask(tasks *list.List) {
	if tasks == nil {
		return
	}
	if r.isLastRoulette {
		r.runTask(*tasks)
		r.slots[r.currentPos] = list.New()
		return
	}
	afterName := r.afterRoulette.name
	runTaskList := list.New()
	for e := tasks.Front(); e != nil; e = e.Next() {
		task := e.Value.(*Task)
		_, ok := task.rouletteSite[afterName]
		if !ok {
			//需要立即执行任务
			runTaskList.PushBack(task)
		}
		// 放入下层轮盘中等待执行
		r.afterRoulette.appendTask(task)
	}
}

// 添加上层转交过来的函数
func (r *Roulette) appendTask(task *Task) {
	index, ok := task.rouletteSite[r.name]
	if !ok {
		go task.Job()
	}
	if r.slots[index] == nil {
		r.slots[index] = list.New()
	}
	r.slots[index].PushBack(task)
	fmt.Printf("%s轮盘落下一个任务\n", r.name)
}

// 删除尚未到期的任务
func (r *Roulette) removeTask(taskKey int64) {
	taskIndex, ok := r.taskKeyMap[taskKey]
	if !ok {
		if r.isLastRoulette {
			return
		}
		r.afterRoulette.removeTask(taskKey)
	}
	l := r.slots[taskIndex]
	for e := l.Front(); e != nil; e = e.Next() {
		task := e.Value.(*Task)
		if task.key == taskKey {
			l.Remove(e)
		}
	}

}

//添加task 根据超时时间计算多久后执行任务 最大时间十年，超过十年会触发panic
func (r *Roulette) addTask(task *Task) {
	/*
			year 2021
			month 8
			day 27
			hour 22
			minute 30
			second 10

			300000

			300,000 / 60 = 5000 0  位置 10 + 0
			5000 / 60 = 83 20  位置 30 + 20
			83 / 24 = 3 11 位置 22 + 11 -> 9 ↓ 1
			3 / 31 = 0 3 位置 27 + 3 + 1

			year 2021
			month 8
			day 31
			hour 9
			minute 50
			second 10

		余数是在本级中的位置，商是上层需要的计算的数字
	*/
	nowRouletteSite := task.delay % r.slotNum // 余数
	nextCircle := task.delay / r.slotNum      // 商
	if nowRouletteSite+r.currentPos >= r.slotNum {
		// 如果超过当前 最大刻度，在上级时间中加一 本级中计算差值
		nextCircle++
		nowRouletteSite = nowRouletteSite + r.currentPos - r.slotNum
	} else {
		nowRouletteSite += r.currentPos
	}
	task.delay = nextCircle
	task.rouletteSite[r.name] = nowRouletteSite
	if nextCircle != 0 {
		if r.name == "year" {
			panic("无法添加超过十年的任务")
		}
		r.beforeRoulette.addTask(task)
		return
	}

	if nowRouletteSite == r.currentPos {
		// 如果执行时间就是当前时间立即调用
		go task.Job()
		return
	}
	if r.slots[nowRouletteSite] == nil {
		r.slots[nowRouletteSite] = list.New()
	}
	r.slots[nowRouletteSite].PushBack(task)
}

func newRoulette(model string, initPointer int64) *Roulette {
	var (
		slotNum        int64
		isLastRoulette bool
	)
	if model == "year" {
		slotNum = 10
	} else if model == "month" {
		slotNum = 12
	} else if model == "day" {
		slotNum = 31
	} else if model == "hour" {
		slotNum = 24
	} else if model == "minute" {
		slotNum = 60
	} else if model == "second" {
		slotNum = 60
		isLastRoulette = true
	} else if model == "millisecond" {
		slotNum = 100 // 暂时用不上
	} else {
		panic("model类型错误")
	}
	return &Roulette{
		name:           model,
		slots:          make([]*list.List, slotNum),
		slotNum:        slotNum,
		currentPos:     initPointer,
		isLastRoulette: isLastRoulette,
		taskKeyMap:     map[int64]int{},
		beforeRoulette: nil,
		afterRoulette:  nil,
	}
}

// TimeWheel 时间轮
type TimeWheel struct {
	interval          time.Duration // 指针每隔多久往前移动一格
	ticker            *time.Ticker  // 时间间隔
	wheel             *Roulette     // 时间轮
	rootWheel         *Roulette     // 最上层时间轮
	taskKeySet        mapset.Set    //taskKey集合
	addTaskChannel    chan Task     // 新增任务channel
	removeTaskChannel chan int64    // 删除任务channel
	stopChannel       chan bool     // 停止定时器channel
}

// 配置信息
type TimeWheelConfig struct {
	Model        string
	TickInterval int64
	BeatSchedule []struct {
		Job     func(interface{})
		JobData interface{}
		delay   int
		crontab *Crontab
	}
}

// NewTimeWheel 调用实例，需要全局唯一，
// model: 模式，就是时间轮层数 年月日时分秒 year, month, day, hour, minute, second
// tickInterval:每次转动的时间间隔
// 使用方法
/*
	tw := NewTimeWheel(&TimeWheelConfig{})
	_ = tw.AppendOnceFunc(oneCallback, 1, 10)
	err := tw.AppendCycleFunc(callbackFunc, 2, Crontab{
		Second: "/5",
	})
	if err != nil {
		tw.Stop()
		println(err.Error())
		return
	}
*/
// 工作大致说明
// TimeWheel.start() 开始入口 ，通过监听*time.Ticker 每秒执行一次 TimeWheel.wheel.tickHandler() 这个方法
// 该方法每次执行都会在时间上 +1秒 ，每一个时间指针都指向一个list.List 链表，链表内存有 Task 对象，被指针指到的链表，其内部所有的 Task 都到了
// 执行时间，
func NewTimeWheel(config *TimeWheelConfig) *TimeWheel {
	var (
		rootRoulette *Roulette // 根节点
		snapRoulette *Roulette // 当前
		lastRoulette *Roulette // 上一个轮盘
	)
	if config.Model == "" {
		config.Model = "second"
	}
	if config.TickInterval == 0 {
		config.TickInterval = 1000000000
	}

	tw := &TimeWheel{
		interval:          time.Duration(config.TickInterval),
		addTaskChannel:    make(chan Task),
		removeTaskChannel: make(chan int64),
		stopChannel:       make(chan bool),
		taskKeySet:        mapset.NewSet(),
	}

	ti := time.Now()
	timeMap := map[string]int64{
		"year":   int64(ti.Year()),
		"month":  int64(ti.Month()),
		"day":    int64(ti.Day()),
		"hour":   int64(ti.Hour()),
		"minute": int64(ti.Minute()),
		"second": int64(ti.Second()),
	}
	modelList := []string{"year", "month", "day", "hour", "minute", "second"}
	for _, defaultModel := range modelList {
		snapRoulette = newRoulette(defaultModel, timeMap[defaultModel])
		if defaultModel == config.Model {
			lastRoulette.afterRoulette = snapRoulette
			snapRoulette.beforeRoulette = lastRoulette
			break
		}
		if defaultModel == "year" {
			rootRoulette = snapRoulette
			lastRoulette = snapRoulette
			continue
		}
		lastRoulette.afterRoulette = snapRoulette
		snapRoulette.beforeRoulette = lastRoulette
		lastRoulette = snapRoulette
	}
	tw.wheel = snapRoulette
	tw.rootWheel = rootRoulette
	tw.ticker = time.NewTicker(tw.interval)
	go tw.start()
	return tw
}

// 开始
func (tw *TimeWheel) start() {
	for {
		select {
		case <-tw.ticker.C:
			tw.wheel.tickHandler()
			println(tw.PrintTime())
		case task := <-tw.addTaskChannel:
			tw.wheel.addTask(&task)
		case key := <-tw.removeTaskChannel:
			tw.rootWheel.removeTask(key)
		case <-tw.stopChannel:
			tw.ticker.Stop()
			return
		}
	}
}

// 停止
func (tw *TimeWheel) Stop() {
	tw.stopChannel <- true
}

// 添加单次任务
func (tw *TimeWheel) AppendOnceFunc(job func(interface{}), jobData interface{}, expiredTime interface{}) error {
	timeParams, err := tw.expiredTimeParsing(expiredTime)
	if err != nil {
		return err
	}
	if timeParams > 10*365*24*60*60 {

		return errors.New("时间最长不能超过十年")
	}
	taskKey := tw.randomTaskKey()
	tw.addTask(job, jobData, timeParams, taskKey, nil)
	return nil
}

// 添加重复任务
func (tw *TimeWheel) AppendCycleFunc(job func(interface{}), jobData interface{}, expiredTime Crontab) error {
	timeParams, err := expiredTime.getNextExecTime(tw.getTimeDict())
	if err != nil {
		return err
	}
	if timeParams > 10*365*24*60*60 {
		return errors.New("时间最长不能超过十年")
	}
	taskKey := tw.randomTaskKey()
	tw.addTask(job, jobData, timeParams, taskKey, &expiredTime)
	return nil
}

// 删除指定的回调任务
func (tw *TimeWheel) RemoveTask(taskKey int64) {
	if !tw.taskKeySet.Contains(taskKey) {
		// taskKey 不存在
		return
	}
	tw.removeTaskChannel <- taskKey
}

// 统一处理回调函数，如果想在执行回调函数的时候做什么事情，就在这修改
func (tw *TimeWheel) addTask(job func(interface{}), jobData interface{}, expiredTime int64, taskKey int64, cycle *Crontab) {
	var taskJob func()
	if cycle != nil {
		taskJob = func() {
			fmt.Println(fmt.Sprintf("%s 执行 %s 函数", time.Now().Format("2006-01-02 15:04:05"), GetFunctionName(job)))
			if !cycle.isCycle() {
				// 如果下次调用时间每次都不相等的话，就需要重新获取到期时间
				expiredTime, _ = cycle.getNextExecTime(tw.getTimeDict())
			}
			tw.addTask(job, jobData, expiredTime, taskKey, cycle)
			job(jobData)

		}
	} else {
		taskJob = func() {
			fmt.Println(fmt.Sprintf("%s执行 %s 函数", time.Now().Format("2006-01-02 15:04:05"), GetFunctionName(job)))
			tw.taskKeySet.Remove(taskKey)
			job(jobData)
		}
	}
	tw.taskKeySet.Add(taskKey)
	//tw.wheel.addTask(&Task{
	//	delay:        expiredTime,
	//	rouletteSite: map[string]int64{},
	//	key:          taskKey,
	//	Job:          taskJob,
	//	crontab:      cycle,
	//})
	tw.addTaskChannel <- Task{
		delay:        expiredTime,
		rouletteSite: map[string]int64{},
		key:          taskKey,
		Job:          taskJob,
		crontab:      cycle,
	}
}

// 获取随机数字
func (tw *TimeWheel) randomTaskKey() (key int64) {
	rand.Seed(time.Now().Unix())
	for {
		key = rand.Int63()
		if !tw.taskKeySet.Contains(key) {
			return key
		}
	}

}

// 解析 AppendOnceFunc 传入的 expiredTime 参数
func (tw *TimeWheel) expiredTimeParsing(timeParams interface{}) (int64, error) {
	if timeInt, intOk := timeParams.(int); intOk {
		return int64(timeInt), nil
	} else if timeInt64, IntOk := timeParams.(int64); IntOk {
		return timeInt64, nil
	} else if timeStr, StrOk := timeParams.(string); StrOk {
		stamp, err := time.ParseInLocation("2006-01-02 15:04:05", timeStr, time.Local)
		if err != nil {
			return 0, err
		}
		return stamp.Unix(), nil
	}
	return 0, errors.New("过期时间类型错误,目前只支持int,int64,string类型")
}

// 获取当前定时器时间 集合
func (tw *TimeWheel) getTime() (year, month, day, hour, minute, second int64) {
	year = tw.rootWheel.currentPos
	month = tw.rootWheel.afterRoulette.currentPos
	day = tw.rootWheel.afterRoulette.afterRoulette.currentPos
	hour = tw.rootWheel.afterRoulette.afterRoulette.afterRoulette.currentPos
	minute = tw.rootWheel.afterRoulette.afterRoulette.afterRoulette.afterRoulette.currentPos
	second = tw.rootWheel.afterRoulette.afterRoulette.afterRoulette.afterRoulette.afterRoulette.currentPos
	return
}

//  获取当前定时器时间 字符串
func (tw *TimeWheel) PrintTime() string {
	year := tw.rootWheel.currentPos
	month := tw.rootWheel.afterRoulette.currentPos
	day := tw.rootWheel.afterRoulette.afterRoulette.currentPos
	hour := tw.rootWheel.afterRoulette.afterRoulette.afterRoulette.currentPos
	minute := tw.rootWheel.afterRoulette.afterRoulette.afterRoulette.afterRoulette.currentPos
	second := tw.rootWheel.afterRoulette.afterRoulette.afterRoulette.afterRoulette.afterRoulette.currentPos
	return fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second)
}

// 获取当前定时器时间 timestamp 对象
func (tw *TimeWheel) getTimeDict() (result timestamp) {
	result = timestamp{}
	result.year = int(tw.rootWheel.currentPos)
	result.month = int(tw.rootWheel.afterRoulette.currentPos)
	result.day = int(tw.rootWheel.afterRoulette.afterRoulette.currentPos)
	result.hour = int(tw.rootWheel.afterRoulette.afterRoulette.afterRoulette.currentPos)
	result.minute = int(tw.rootWheel.afterRoulette.afterRoulette.afterRoulette.afterRoulette.currentPos)
	result.second = int(tw.rootWheel.afterRoulette.afterRoulette.afterRoulette.afterRoulette.afterRoulette.currentPos)
	return result
}

// timestamp 时间对象++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
type timestamp struct {
	year   int
	month  int
	day    int
	hour   int
	minute int
	second int
}

// 指定的时间刻度+1
func (t *timestamp) addUp(timeType string) {
	switch timeType {
	case "year":
		t.year++
	case "month":
		t.month++
		if t.month == 13 {
			t.month = 1
			t.year++
		}
	case "day":
		t.day++
		if t.day == int(getMonthDay(int64(t.year), int64(t.month)))+1 {
			t.day = 1
			t.addUp("month")
		}
	case "hour":
		t.hour++
		if t.hour == 24 {
			t.hour = 0
			t.addUp("day")
		}
	case "minute":
		t.minute++
		if t.minute == 60 {
			t.minute = 0
			t.addUp("hour")
		}
	case "second":
		t.second++
		if t.second == 60 {
			t.second = 0
			t.addUp("minute")
		}
	default:
		panic("错误类型")
	}
}

// 获取时间差
func (t timestamp) stamp(datetime timestamp) int64 {
	// t 目标天数
	// 当前时间
	// 计算datetime时间到t 时间相差的秒数
	// 只能计算t比datetime大的时间
	days := -1
	stamp := 0
	// year ==============================
	a := datetime.year
	//if t.year+t.month+t.day < datetime.year+datetime.month+datetime.day {
	//	a = t.year
	//	b = datetime.year
	//}
	for {
		if a >= t.year {
			break
		}
		a++
		if leapYear(int64(a)) {
			days += 366
		} else {
			days += 365
		}
	}
	// t ==================================
	a = 1
	tDay := 0
	for {
		if a == t.month {
			tDay += t.day
			break
		}
		tDay += int(getMonthDay(int64(t.year), int64(a)))
		a++
	}
	// datetime ===========================
	a = 1
	datetimeDay := 0
	for {
		if a == datetime.month {
			datetimeDay += datetime.day
			break
		}
		datetimeDay += int(getMonthDay(int64(t.year), int64(a)))
		a++
	}
	days += tDay - datetimeDay
	if leapYear(int64(datetime.year)) {
		days++
	}
	stamp += days * (24 * 60 * 60)
	a = t.hour*3600 + t.minute*60 + t.second
	b := datetime.hour*3600 + datetime.minute*60 + datetime.second
	stamp += a - b
	x := a - b
	if a < b {
		// 如果t的时分秒比datetime的时分秒小，那需要加一天
		stamp += 86400
		x += 86400
	}
	return int64(stamp)
}

// 使用时间戳获取时间差
func (t timestamp) sysStamp(datetime timestamp) int64 {
	datetimeStamp, _ := time.ParseInLocation("2006-01-02 15:04:05", datetime.PrintTime(), time.Local)
	selfStamp, _ := time.ParseInLocation("2006-01-02 15:04:05", t.PrintTime(), time.Local)

	return selfStamp.Unix() - datetimeStamp.Unix()
}

// 格式化时间字符串 2020-01-01 12:00:00
func (t timestamp) PrintTime() string {
	return fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d", t.year, t.month, t.day, t.hour, t.minute, t.second)
}

// Crontab 时间执行表 +++++++++++++++++++++++++++++++++++++++++++++++++++
// 字符串 按照给定的数字，当时间到给定的刻度就会执行
// 比如 Crontab{Minute:10,Second:30} 每个小时的十分三十秒的时候就会执行
// 支持一次传入多个时间点 Crontab{Minute:"10,11,12",Second:30}每个小时的10：30，11：30，12：30三个时间点执行
// 连续时间点可以用-表示 Minute:"10-12" 代表 Minute:"10,11,12"
// 也可以 /5表示当时间点可以被5整除的时候就执行任务 前面可以写自己指定的时间段 默认的是当前时间段的起止 比如 秒 就是0-59
// 10-20/5 表示当时间点在 10 15 20 这三个时间点执行任务
// 参照python的celery的crontab实现的
type Crontab struct {
	Second        string
	Minute        string
	Hour          string
	Day           string
	Month         string
	Year          string
	second        []int
	minute        []int
	hour          []int
	day           []int
	month         []int
	year          []int
	timeDict      map[string][]int
	init          bool
	beforeRunTime timestamp //上次调用时间
}

// 获取执行延时
func (c *Crontab) getNextExecTime(TimeDict timestamp) (int64, error) {
	if !c.init {
		// 初始化 解析字符串
		var abc []int
		c.timeDict = make(map[string][]int)
		if abc = splitArgs(c.Year, "year"); len(abc) != 0 {
			c.year = abc
			c.timeDict["year"] = abc
		}
		if abc = splitArgs(c.Month, "month"); len(abc) != 0 {
			c.month = abc
			c.timeDict["month"] = abc
		}
		if abc = splitArgs(c.Day, "day"); len(abc) != 0 {
			c.day = abc
			c.timeDict["day"] = abc
		}
		if abc = splitArgs(c.Hour, "hour"); len(abc) != 0 {
			c.hour = abc
			c.timeDict["hour"] = abc
		}
		if abc = splitArgs(c.Minute, "minute"); len(abc) != 0 {
			c.minute = abc
			c.timeDict["minute"] = abc
		}
		if abc = splitArgs(c.Second, "second"); len(abc) != 0 {
			c.second = abc
			c.timeDict["second"] = abc
		}
		c.init = true
	}

	if len(c.timeDict) == 0 {
		//默认每分钟执行一次
		return 60, nil
	}
	//TimeDict := timestamp{
	//	year:   2021,
	//	month:  9,
	//	day:    1,
	//	hour:   15,
	//	minute: 54,
	//	second: 21,
	//}
	ti := c.nextTime(TimeDict)
	c.beforeRunTime = ti
	return ti.sysStamp(TimeDict), nil
}

// 按照给定的规则找到与传入时间 最近的执行时间
// 如果上次执行时间和当前时间相同那就返回的是下次的时间
// 如何才能返回下一个执行时间点？
func (c *Crontab) nextTime(TimeDict timestamp) timestamp {
	if c.beforeRunTime.PrintTime() == TimeDict.PrintTime() {
		TimeDict.addUp("second")
	}

	ti := timestamp{
		year:   -1,
		month:  -1,
		day:    -1,
		hour:   -1,
		minute: -1,
		second: -1,
	}
	// year
	timeIntList, ok := c.timeDict["year"]
	year := TimeDict.year
	if !ok {
		ti.year = year
	} else {
		for _, x := range timeIntList {
			if year <= x {
				ti.year = x
				break
			}
		}
		if ti.year == -1 {
			ti.year = timeIntList[0]
		}
	}
	// month
	timeIntList, ok = c.timeDict["month"]
	month := TimeDict.month
	if !ok {
		ti.month = month
	} else {
		for _, x := range timeIntList {
			if month <= x {
				ti.month = x
				break
			}
		}
		if ti.month == -1 {
			ti.month = timeIntList[0]
			ti.addUp("year")
		}
	}
	// day
	timeIntList, ok = c.timeDict["day"]
	day := TimeDict.day
	if !ok {
		ti.day = day
	} else {
		for _, x := range timeIntList {
			if day <= x {
				ti.day = x
				break
			}
		}
		if ti.day == -1 {
			ti.day = timeIntList[0]
			ti.addUp("month")
		}
	}
	// hour
	timeIntList, ok = c.timeDict["hour"]
	hour := TimeDict.hour
	if !ok {
		ti.hour = hour
	} else {
		for _, x := range timeIntList {
			if hour <= x {
				ti.hour = x
				break
			}
		}
		if ti.hour == -1 {
			ti.hour = timeIntList[0]
			ti.addUp("day")
		}
	}
	// minute
	timeIntList, ok = c.timeDict["minute"]
	minute := TimeDict.minute
	if !ok {
		ti.minute = minute
	} else {
		for _, x := range timeIntList {
			if minute <= x {
				ti.minute = x
				break
			}
		}
		if ti.minute == -1 {
			ti.minute = timeIntList[0]
			ti.addUp("hour")
		}
	}
	// second
	timeIntList, ok = c.timeDict["second"]
	second := TimeDict.second
	if !ok {
		ti.second = second
	} else {
		for _, x := range timeIntList {
			if second <= x {
				ti.second = x
				break
			}
		}
		if ti.second == -1 {
			ti.second = timeIntList[0]
			ti.addUp("minute")
		}
	}

	return ti
}

// 每次执行任务的时间差是否相等 相同返回true 不同返回false
func (c *Crontab) isCycle() bool {

	return false
}

// 切割字符串，根据输入将执行的时间点以切片的形式返回
func splitArgs(args, timeType string) []int {
	/* 1,2,3,4,5
	/2 == 0
	0-10/2 == 0
	0-10 0,1,2,3,4,5,6,7,8,9,10
	*/

	if args == "" {
		return []int{}
	}
	s := strings.Split(args, ",")
	var result []int
	for _, timeStr := range s {
		isTrue := true
		xIndex := -1 // /
		yIndex := -1 // -
		var cache []int
		var special []int
		for _, a := range timeStr {
			if !unicode.IsDigit(a) {
				isTrue = false
				if a == 47 {
					// /字符
					if xIndex != -1 {
						panic(fmt.Sprintf("%s字符格式错误%s", timeType, args))
					}
					if len(cache) > 0 {
						special = append(special, listToInt(cache))
					}
					special = append(special, -1)
					cache = cache[:0]
					xIndex = len(special) - 1
				}
				if a == 45 {
					// -字符
					if yIndex != -1 {
						panic(fmt.Sprintf("%s字符格式错误%s", timeType, args))
					}
					special = append(special, listToInt(cache))
					special = append(special, -2)
					cache = cache[:0]
					yIndex = len(special) - 1
				}
				continue
			}
			cache = append(cache, strToInt(a))
		}

		if !isTrue {
			special = append(special, listToInt(cache))
			if xIndex != -1 {
				var e, f int
				if xIndex == 0 {
					e, f = timeRange(timeType)
				} else {
					e, f = special[0], special[2]
				}
				base := special[xIndex+1]
				// 根据起始时间计算可被底数整除的时间点
				for e <= f {
					if e%base == 0 {
						result = append(result, e)
					}
					e++
				}
			} else {
				e, f := special[0], special[2]
				for e <= f {
					result = append(result, e)
					e++
				}
			}
		} else {
			result = append(result, listToInt(cache))
		}
	}

	return result
}

// rune 映射成数字
func strToInt(a rune) int {
	switch a {
	case 48:
		return 0
	case 49:
		return 1
	case 50:
		return 2
	case 51:
		return 3
	case 52:
		return 4
	case 53:
		return 5
	case 54:
		return 6
	case 55:
		return 7
	case 56:
		return 8
	case 57:
		return 9
	}
	panic("")
}

// 数字切片转整数
func listToInt(data []int) int {
	l := len(data) - 1
	result := 0
	for index, value := range data {
		result += pow(10, l-index) * value
	}
	return result
}

// 计算次方
func pow(x, n int) int {
	ret := 1 // 结果初始为0次方的值，整数0次方为1。如果是矩阵，则为单元矩阵。
	for n != 0 {
		if n%2 != 0 {
			ret = ret * x
		}
		n /= 2
		x = x * x
	}
	return ret
}

// 每个时间轮盘的起止时间点
func timeRange(timeType string) (start, end int) {
	switch timeType {
	case "year":
		return 2021, 2031
	case "month":
		return 1, 12
	case "day":
		return 1, 31 // 2月份的处理放在计算时间间隔的时候
	case "hour":
		return 0, 23
	case "minute":
		return 0, 59
	case "second":
		return 0, 59
	}
	panic("错误时间类型")
}

// 获取函数名
func GetFunctionName(i interface{}, seps ...rune) string {
	// 获取函数名称
	fn := runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()

	// 用 seps 进行分割
	fields := strings.FieldsFunc(fn, func(sep rune) bool {
		for _, s := range seps {
			if sep == s {
				return true
			}
		}
		return false
	})

	if size := len(fields); size > 0 {
		return fields[size-1]
	}
	return ""
}
