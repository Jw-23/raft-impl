package tests

import (
	"sync"
	"testing"
)

// (这里是您的 ParallelReduce 函数... 我省略了，但它应该在这里)

// --- 基准测试 ---

// 公共的测试数据，可以在多个基准测试之间共享
var testData = make([]int, 100000)

// 使用 init 函数来初始化测试数据，这只会在包加载时运行一次
func init() {
	for i := range testData {
		testData[i] = i
	}
}

// 并行版本的基准测试
func BenchmarkParallelReduce(b *testing.B) {
	// 使用 b.ResetTimer() 如果准备工作在测试函数内部
	// 但我们用了 init，所以这里不需要了

	// 核心逻辑放在 b.N 循环内
	for b.Loop() {
		// 这里只放要测试的核心函数，不要有 fmt.Printf
		_ = ParallelReduce(testData, 100, // chunkSize 调整一下
			func(chunk []int) int {
				sum := 0
				for _, i := range chunk {
					sum += i
				}
				return sum
			},
			func(a, b int) int {
				return a + b
			},
			0,
		)
	}
}

// 串行版本的基准测试
func BenchmarkSerialReduce(b *testing.B) {
	for b.Loop() {
		sum := 0
		for _, i := range testData {
			sum += i
		}
		// 把 sum 赋值给一个 dummy 变量，防止编译器优化掉整个循环
		_ = sum
	}
}

func ParallelReduce[T any, S any](items []T, chunkSize int, chunkReducer func(chunk []T) S, finalReducer func(a S, b S) S, initial S) S {
	var wg sync.WaitGroup
	n := len(items) / chunkSize
	results := make(chan S, n)
	for i := range n {
		wg.Add(1)
		var chunk []T
		if i == n-1 {
			chunk = items[i*chunkSize:]
		} else {
			chunk = items[i*chunkSize : (i+1)*chunkSize]
		}
		go func(chunk []T) {
			defer wg.Done()
			results <- chunkReducer(chunk)
		}(chunk)
	}
	go func() {
		wg.Wait()
		close(results)
	}()
	current := initial
	for v := range results {
		current = finalReducer(current, v)
	}
	return current
}
