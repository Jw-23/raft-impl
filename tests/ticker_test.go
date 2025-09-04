package tests

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestTimeoutCorrect(t *testing.T) {
	// Worker 现在只负责执行任务并返回结果。
	// 它不再需要管理 context 或自己的 done channel。
	worker := func() int {
		time.Sleep(1 * time.Second)
		return 49
	}

	// 1. 创建带超时的 context
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	// 2. 创建用于接收结果的 channel
	resultChan := make(chan int)

	// 3. 启动 worker goroutine
	go func() {
		// 在 select 中监听 context 的 done 信号，
		// 避免在 context 取消后，还向 resultChan 发送数据。
		select {
		case resultChan <- worker():
			// 任务正常完成，发送结果
		case <-ctx.Done():
			// Context 被取消，什么也不做，goroutine 直接退出
			return
		}
	}()

	// 4. 主 goroutine 使用 select 来等待结果或超时
	select {
	case v := <-resultChan:
		fmt.Printf("receive value : %d\n", v)
	case <-ctx.Done():
		// ctx.Err() 会返回导致 context 结束的原因
		fmt.Printf("timed out: %v\n", ctx.Err())
	}
}

func TestTicker(t *testing.T) {
	var wg sync.WaitGroup
	d := time.Second * 3
	sign := make(chan struct{})

	worker := func() {
		// 在实际场景中，这里可能会有一些耗时操作
		fmt.Println("--- work now ---")
	}

	var ticker = time.NewTicker(d)
	defer ticker.Stop()

	// 1. 我们要等待 worker goroutine 结束，所以在这里 Add(1)
	wg.Add(1)
	go func() {
		// 2. 在 goroutine 退出时，调用 Done()
		defer wg.Done()
		fmt.Println("Worker goroutine started. Waiting for ticks or stop signal.")
		for {
			select {
			case <-ticker.C:
				worker()
			case <-sign:
				// 收到停止信号，准备退出
				fmt.Println("Stop signal received. Worker goroutine exiting.")
				return // 退出 for 循环，goroutine 即将结束
			}
		}
	}()

	// 启动另一个 goroutine，在 10 秒后发送停止信号
	// 这个 goroutine 不需要被 WaitGroup 等待，因为它只是一个触发器
	go func() {
		time.Sleep(time.Second * 10)
		fmt.Println("Sending stop signal...")
		// 使用 close(channel) 是一个更通用的、可以广播给多个接收者的方式
		// 在这里发送一个空结构体效果一样
		close(sign)
	}()

	// 3. 等待 worker goroutine 执行完毕（即它收到 sign 信号并返回）
	wg.Wait()
	fmt.Println("Test finished.")
}
