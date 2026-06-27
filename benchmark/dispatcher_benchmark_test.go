package benchmark

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	defaultBatchSize = 64
	cpuWorkRounds    = 128
	scoreTaskCount   = 32768
)

var sink uint64

type workload struct {
	name string
	fn   func(int) uint64
}

var workloads = []workload{
	{name: "TinyWork", fn: tinyWork},
	{name: "CPUWork", fn: cpuWork},
}

func BenchmarkSequential(b *testing.B) {
	for _, work := range workloads {
		b.Run(work.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			var sum uint64
			for i := 0; i < b.N; i++ {
				sum += work.fn(i)
			}

			atomic.AddUint64(&sink, sum)
			reportTasksPerSecond(b, b.N)
		})
	}
}

func BenchmarkChannelDispatcher(b *testing.B) {
	for _, work := range workloads {
		b.Run(work.name, func(b *testing.B) {
			workers := workerCount()
			tasks := make([]int, b.N)
			for i := range tasks {
				tasks[i] = i
			}

			b.ReportAllocs()
			b.ReportMetric(float64(workers), "workers")
			b.ResetTimer()

			sum := runChannelDispatcher(context.Background(), workers, tasks, work.fn)

			atomic.AddUint64(&sink, sum)
			reportTasksPerSecond(b, len(tasks))
		})
	}
}

func BenchmarkAtomicSliceDispatcher(b *testing.B) {
	for _, work := range workloads {
		b.Run(work.name, func(b *testing.B) {
			workers := workerCount()
			tasks := make([]int, b.N)
			for i := range tasks {
				tasks[i] = i
			}

			b.ReportAllocs()
			b.ReportMetric(float64(workers), "workers")
			b.ResetTimer()

			sum := runAtomicSliceDispatcher(context.Background(), workers, tasks, work.fn)

			atomic.AddUint64(&sink, sum)
			reportTasksPerSecond(b, len(tasks))
		})
	}
}

func BenchmarkBatchDispatcher(b *testing.B) {
	for _, work := range workloads {
		b.Run(work.name, func(b *testing.B) {
			workers := workerCount()
			tasks := make([]int, b.N)
			for i := range tasks {
				tasks[i] = i
			}
			batches := batchTasks(tasks, defaultBatchSize)

			b.ReportAllocs()
			b.ReportMetric(float64(workers), "workers")
			b.ReportMetric(float64(defaultBatchSize), "batch_size")
			b.ResetTimer()

			sum := runBatchDispatcher(context.Background(), workers, batches, work.fn)

			atomic.AddUint64(&sink, sum)
			reportTasksPerSecond(b, len(tasks))
		})
	}
}

func BenchmarkEvaluationScore(b *testing.B) {
	cases := []struct {
		name string
		run  func(context.Context, int, []int, func(int) uint64) uint64
	}{
		{name: "ChannelCPUWork", run: runChannelDispatcher},
		{name: "AtomicSliceCPUWork", run: runAtomicSliceDispatcher},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			workers := workerCount()
			tasks := make([]int, scoreTaskCount)
			for i := range tasks {
				tasks[i] = i
			}

			b.ReportAllocs()
			b.ReportMetric(float64(workers), "workers")
			b.ResetTimer()

			var speedupTotal float64
			for i := 0; i < b.N; i++ {
				sequentialDuration, sequentialSum := measure(func() uint64 {
					return runSequential(tasks, cpuWork)
				})
				dispatcherDuration, dispatcherSum := measure(func() uint64 {
					return tc.run(context.Background(), workers, tasks, cpuWork)
				})

				atomic.AddUint64(&sink, sequentialSum+dispatcherSum)
				speedupTotal += float64(sequentialDuration) / float64(dispatcherDuration)
			}

			speedup := speedupTotal / float64(b.N)
			b.ReportMetric(speedup, "speedup")
			b.ReportMetric(speedup/float64(workers), "efficiency")
		})
	}

	b.Run("BatchCPUWork", func(b *testing.B) {
		workers := workerCount()
		tasks := make([]int, scoreTaskCount)
		for i := range tasks {
			tasks[i] = i
		}
		batches := batchTasks(tasks, defaultBatchSize)

		b.ReportAllocs()
		b.ReportMetric(float64(workers), "workers")
		b.ReportMetric(float64(defaultBatchSize), "batch_size")
		b.ResetTimer()

		var speedupTotal float64
		for i := 0; i < b.N; i++ {
			sequentialDuration, sequentialSum := measure(func() uint64 {
				return runSequential(tasks, cpuWork)
			})
			dispatcherDuration, dispatcherSum := measure(func() uint64 {
				return runBatchDispatcher(context.Background(), workers, batches, cpuWork)
			})

			atomic.AddUint64(&sink, sequentialSum+dispatcherSum)
			speedupTotal += float64(sequentialDuration) / float64(dispatcherDuration)
		}

		speedup := speedupTotal / float64(b.N)
		b.ReportMetric(speedup, "speedup")
		b.ReportMetric(speedup/float64(workers), "efficiency")
	})
}

func runSequential(tasks []int, fn func(int) uint64) uint64 {
	var sum uint64
	for _, task := range tasks {
		sum += fn(task)
	}
	return sum
}

func runChannelDispatcher(ctx context.Context, workers int, tasks []int, fn func(int) uint64) uint64 {
	jobs := make(chan int, workers*2)
	results := make(chan uint64, workers*2)

	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for task := range jobs {
				select {
				case <-ctx.Done():
					return
				default:
				}
				results <- fn(task)
			}
		}()
	}

	go func() {
	submitLoop:
		for _, task := range tasks {
			select {
			case <-ctx.Done():
				break submitLoop
			case jobs <- task:
			}
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()

	var sum uint64
	for result := range results {
		sum += result
	}
	return sum
}

func runAtomicSliceDispatcher(ctx context.Context, workers int, tasks []int, fn func(int) uint64) uint64 {
	var next atomic.Int64
	var sum atomic.Uint64
	var wg sync.WaitGroup

	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			local := uint64(0)
			for {
				if ctx.Err() != nil {
					break
				}
				index := int(next.Add(1)) - 1
				if index >= len(tasks) {
					break
				}
				local += fn(tasks[index])
			}
			sum.Add(local)
		}()
	}

	wg.Wait()
	return sum.Load()
}

func runBatchDispatcher(ctx context.Context, workers int, batches [][]int, fn func(int) uint64) uint64 {
	var next atomic.Int64
	var sum atomic.Uint64
	var wg sync.WaitGroup

	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			local := uint64(0)
			for {
				if ctx.Err() != nil {
					break
				}
				index := int(next.Add(1)) - 1
				if index >= len(batches) {
					break
				}
				for _, task := range batches[index] {
					local += fn(task)
				}
			}
			sum.Add(local)
		}()
	}

	wg.Wait()
	return sum.Load()
}

func batchTasks(tasks []int, batchSize int) [][]int {
	batches := make([][]int, 0, (len(tasks)+batchSize-1)/batchSize)
	for start := 0; start < len(tasks); start += batchSize {
		end := start + batchSize
		if end > len(tasks) {
			end = len(tasks)
		}
		batches = append(batches, tasks[start:end])
	}
	return batches
}

func measure(fn func() uint64) (time.Duration, uint64) {
	started := time.Now()
	sum := fn()
	return time.Since(started), sum
}

func workerCount() int {
	workers := runtime.GOMAXPROCS(0) - 1
	if workers < 1 {
		return 1
	}
	return workers
}

func reportTasksPerSecond(b *testing.B, tasks int) {
	elapsed := b.Elapsed().Seconds()
	if elapsed <= 0 {
		return
	}
	b.ReportMetric(float64(tasks)/elapsed, "tasks/s")
}

func tinyWork(value int) uint64 {
	return uint64(value*31 + 7)
}

func cpuWork(value int) uint64 {
	x := uint64(value) + 0x9e3779b97f4a7c15
	for i := 0; i < cpuWorkRounds; i++ {
		x ^= x >> 30
		x *= 0xbf58476d1ce4e5b9
		x ^= x >> 27
		x *= 0x94d049bb133111eb
		x ^= x >> 31
	}
	return x
}
