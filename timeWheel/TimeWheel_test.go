package timeWheel

import (
	"fmt"
	"testing"
	"time"
)

var callbackIndex int

func TestCrontab(t *testing.T) {
	callbackIndex++
	tw := NewTimeWheel(&TimeWheelConfig{})
	//fmt.Println(tw.PrintTime())
	key, _ := tw.AppendOnceFunc(oneCallback, 1, 10)
	_, err := tw.AppendCycleFunc(callbackFunc, 2, Crontab{Second: "/5"})
	fmt.Printf("添加任务完成")
	if err != nil {
		tw.Stop()
		println(err.Error())
		return
	}
	tw.RemoveTask(key)
	time.Sleep(time.Minute)
}
func oneCallback(data interface{}) {
	println(fmt.Sprintf("单次回调函数执行：执行序号%d", data))
}
func callbackFunc(data interface{}) {
	println(fmt.Sprintf("测试回调函数执行：执行序号%d", callbackIndex))
	callbackIndex++
}
