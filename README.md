# Celestial

Celestial is a small generic task dispatcher for Go.

It runs many independent tasks with a fixed worker pool and streams results as soon as tasks finish. It is not tied to any language, game, data shape, or domain.

## Features

- Fixed-size worker pool.
- Tasks from slices or channels.
- Function-based or interface-based executors.
- Result stream with worker index, task id, error, and duration.
- Context cancellation.
- Optional stop-on-first-error.
- Optional process pool for JSON Lines workers.

## Install

```bash
go get github.com/Haruko386/Celestial
```

For local development:

```go
replace github.com/Haruko386/Celestial => ../Celestial
```

## Main APIs

```go
dispatcher := celestial.New[Task, Value](celestial.Config{
	Workers:     8,
	QueueSize:   16,
	StopOnError: true,
})
```

- `Run(ctx, tasks, handler)` runs tasks from a channel.
- `RunSlice(ctx, tasks, handler)` runs tasks from a slice.
- `RunWith(ctx, tasks, executor)` runs tasks with an `Executor`.
- `RunSliceWith(ctx, tasks, executor)` runs slice tasks with an `Executor`.
- `run.Results()` streams task results.
- `run.Stop()` cancels the run.
- `run.Err()` returns the first run error.

Use `RunSlice` for in-memory tasks. It uses an atomic index fast path and avoids the channel producer step. Use `Run` when tasks are produced over time.

## Function Executor

```go
package main

import (
	"context"
	"fmt"

	celestial "github.com/Haruko386/Celestial"
)

func main() {
	dispatcher := celestial.New[int, int](celestial.Config{
		Workers: celestial.DefaultWorkerCount(),
	})

	run := dispatcher.RunSlice(context.Background(), []int{1, 2, 3, 4}, func(ctx context.Context, worker celestial.Worker, task int) (int, error) {
		return task * task, nil
	})

	for result := range run.Results() {
		if result.Err != nil {
			fmt.Println("task failed:", result.Err)
			continue
		}
		fmt.Printf("worker=%d task=%d value=%d\n", result.WorkerIndex, result.TaskID, result.Value)
	}

	if err := run.Err(); err != nil {
		fmt.Println("run failed:", err)
	}
}
```

## Interface Executor

Use `Executor` when task execution needs state.

```go
type ImageJob struct {
	Path string
}

type ImageResult struct {
	Path string
	OK   bool
}

type ImageExecutor struct {
	Quality int
}

func (e *ImageExecutor) Execute(ctx context.Context, worker celestial.Worker, job ImageJob) (ImageResult, error) {
	// Do CPU-bound or IO-bound work here.
	return ImageResult{Path: job.Path, OK: true}, nil
}

dispatcher := celestial.New[ImageJob, ImageResult](celestial.Config{Workers: 4})
run := dispatcher.RunSliceWith(context.Background(), jobs, &ImageExecutor{Quality: 90})
```

## Channel Tasks

```go
tasks := make(chan string)

go func() {
	defer close(tasks)
	for _, path := range paths {
		tasks <- path
	}
}()

run := dispatcher.Run(context.Background(), tasks, handler)
```

## Batching

Batch small tasks to reduce scheduling overhead.

```go
batches, err := celestial.BatchSlice(files, 64)
if err != nil {
	panic(err)
}

dispatcher := celestial.New[[]string, int](celestial.Config{Workers: 8})
run := dispatcher.RunSlice(context.Background(), batches, func(ctx context.Context, worker celestial.Worker, batch []string) (int, error) {
	processed := 0
	for _, file := range batch {
		_ = file
		processed++
	}
	return processed, nil
})
```

## Range Helper

`Range` is only a helper for numeric workloads. You can ignore it if your tasks are not ranges.

```go
ranges, err := celestial.SplitRange(0, 1_000_000, 10_000)
if err != nil {
	panic(err)
}

run := dispatcher.RunSlice(context.Background(), ranges, func(ctx context.Context, worker celestial.Worker, r celestial.Range) (int64, error) {
	return r.Len(), nil
})
```

For very large ranges:

```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

tasks, errs := celestial.StreamRange(ctx, 0, 1_000_000_000, 10_000)
run := dispatcher.Run(ctx, tasks, rangeHandler)

for result := range run.Results() {
	_ = result
}

cancel()
if err := <-errs; err != nil {
	_ = err
}
```

## Process Workers

The `process` package manages long-running child processes that speak JSON Lines:

- one JSON request per stdin line
- one JSON response per stdout line

The child process can be written in any language.

```go
pool, err := process.NewPool[Request, Response](ctx, 4, func(index int) (process.Command, error) {
	return process.Command{
		Name: "worker-binary",
		Args: []string{"--json-lines"},
		Env:  []string{"APP_ENV=production"},
	}, nil
})
if err != nil {
	panic(err)
}
defer pool.Close()

dispatcher := celestial.New[Request, Response](celestial.Config{Workers: pool.Len()})
run := dispatcher.RunSlice(ctx, requests, func(ctx context.Context, worker celestial.Worker, req Request) (Response, error) {
	return pool.CallAt(worker.Index, req)
})
```

## Config

```go
type Config struct {
	Workers     int
	QueueSize   int
	StopOnError bool
}
```

- `Workers`: number of concurrent workers. Default: `DefaultWorkerCount()`.
- `QueueSize`: task queue size. Default: `Workers * 2`.
- `StopOnError`: cancel the run after the first task error.

## Performance Notes

- Prefer `RunSlice` for fixed task lists.
- Prefer `Run` for streaming producers.
- Use `BatchSlice` if each task is extremely small.
- Always consume `run.Results()` unless the run is cancelled.
- Make handlers respect `ctx.Done()` for fast cancellation.
