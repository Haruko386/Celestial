// Package celestial provides a small concurrent task dispatcher.
package celestial

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// Worker identifies the goroutine that is executing a task.
type Worker struct {
	Index int
}

// Executor runs one task.
type Executor[Task any, Value any] interface {
	Execute(context.Context, Worker, Task) (Value, error)
}

// Handler executes one task on one worker.
type Handler[Task any, Value any] func(context.Context, Worker, Task) (Value, error)

// Execute adapts Handler to Executor.
func (h Handler[Task, Value]) Execute(ctx context.Context, worker Worker, task Task) (Value, error) {
	return h(ctx, worker, task)
}

// Config controls dispatcher concurrency and buffering.
type Config struct {
	// Workers is the number of concurrent worker goroutines.
	// When Workers is zero, DefaultWorkerCount is used.
	Workers int

	// QueueSize is the internal task queue buffer size.
	// When QueueSize is zero, Workers * 2 is used.
	QueueSize int

	// StopOnError cancels the whole run after the first task error.
	StopOnError bool
}

// Dispatcher accepts tasks and fans them out to a fixed set of workers.
type Dispatcher[Task any, Value any] struct {
	config Config
}

// New creates a dispatcher with normalized config.
func New[Task any, Value any](config Config) *Dispatcher[Task, Value] {
	config = normalizeConfig(config)
	return &Dispatcher[Task, Value]{config: config}
}

// Config returns the normalized dispatcher config.
func (d *Dispatcher[Task, Value]) Config() Config {
	return d.config
}

// Run starts a streaming task run.
func (d *Dispatcher[Task, Value]) Run(ctx context.Context, tasks <-chan Task, handle Handler[Task, Value]) *Run[Value] {
	if handle == nil {
		return failedRun[Value](errors.New("celestial: nil handler"))
	}
	return d.RunWith(ctx, tasks, handle)
}

// RunWith starts a streaming task run with an Executor.
func (d *Dispatcher[Task, Value]) RunWith(ctx context.Context, tasks <-chan Task, executor Executor[Task, Value]) *Run[Value] {
	if executor == nil {
		return failedRun[Value](errors.New("celestial: nil executor"))
	}
	ctx, cancel := context.WithCancel(ctx)
	results := make(chan Result[Value], d.config.QueueSize)
	done := make(chan struct{})
	run := &Run[Value]{
		results: results,
		done:    done,
		cancel:  cancel,
	}

	go func() {
		defer close(done)
		defer close(results)

		jobs := make(chan queuedTask[Task], d.config.QueueSize)
		var nextID atomic.Int64
		var wg sync.WaitGroup

		wg.Add(d.config.Workers)
		for i := 0; i < d.config.Workers; i++ {
			go func(index int) {
				defer wg.Done()
				d.work(ctx, cancel, Worker{Index: index}, jobs, results, executor, run)
			}(i)
		}

	submitLoop:
		for {
			var task Task
			select {
			case <-ctx.Done():
				break submitLoop
			case received, ok := <-tasks:
				if !ok {
					break submitLoop
				}
				task = received
			}

			job := queuedTask[Task]{id: nextID.Add(1), value: task}
			select {
			case jobs <- job:
				run.submitted.Add(1)
			case <-ctx.Done():
				break submitLoop
			}
		}

		close(jobs)
		wg.Wait()

		if err := ctx.Err(); err != nil && !errors.Is(err, context.Canceled) {
			run.setErr(err)
		}
	}()

	return run
}

// RunSlice starts a task run from an in-memory slice.
func (d *Dispatcher[Task, Value]) RunSlice(ctx context.Context, tasks []Task, handle Handler[Task, Value]) *Run[Value] {
	if handle == nil {
		return failedRun[Value](errors.New("celestial: nil handler"))
	}
	return d.RunSliceWith(ctx, tasks, handle)
}

// RunSliceWith starts a slice task run with an Executor.
func (d *Dispatcher[Task, Value]) RunSliceWith(ctx context.Context, tasks []Task, executor Executor[Task, Value]) *Run[Value] {
	if executor == nil {
		return failedRun[Value](errors.New("celestial: nil executor"))
	}

	input := make(chan Task)
	go func() {
		defer close(input)
		for _, task := range tasks {
			select {
			case input <- task:
			case <-ctx.Done():
				return
			}
		}
	}()
	return d.RunWith(ctx, input, executor)
}

func (d *Dispatcher[Task, Value]) work(
	ctx context.Context,
	cancel context.CancelFunc,
	worker Worker,
	jobs <-chan queuedTask[Task],
	results chan<- Result[Value],
	executor Executor[Task, Value],
	run *Run[Value],
) {
	for job := range jobs {
		if ctx.Err() != nil {
			return
		}

		started := time.Now()
		value, err := executor.Execute(ctx, worker, job.value)
		finished := time.Now()
		if err != nil {
			run.setErr(err)
			if d.config.StopOnError {
				cancel()
			}
		}

		result := Result[Value]{
			TaskID:      job.id,
			WorkerIndex: worker.Index,
			Value:       value,
			Err:         err,
			StartedAt:   started,
			FinishedAt:  finished,
			Duration:    finished.Sub(started),
		}

		select {
		case results <- result:
			run.completed.Add(1)
		case <-ctx.Done():
			return
		}
	}
}

// DefaultWorkerCount leaves one CPU for coordination when possible.
func DefaultWorkerCount() int {
	workers := runtime.NumCPU() - 1
	if workers < 1 {
		return 1
	}
	return workers
}

func normalizeConfig(config Config) Config {
	if config.Workers < 1 {
		config.Workers = DefaultWorkerCount()
	}
	if config.QueueSize < 1 {
		config.QueueSize = config.Workers * 2
	}
	return config
}

type queuedTask[Task any] struct {
	id    int64
	value Task
}

// Result is emitted once for each completed task.
type Result[Value any] struct {
	TaskID      int64
	WorkerIndex int
	Value       Value
	Err         error
	StartedAt   time.Time
	FinishedAt  time.Time
	Duration    time.Duration
}

// Run is a handle for an active dispatcher run.
type Run[Value any] struct {
	results   <-chan Result[Value]
	done      <-chan struct{}
	cancel    context.CancelFunc
	errMu     sync.Mutex
	err       error
	submitted atomic.Int64
	completed atomic.Int64
}

// Results streams task results until the run finishes.
func (r *Run[Value]) Results() <-chan Result[Value] {
	return r.results
}

// Done is closed after all workers exit and the result channel is closed.
func (r *Run[Value]) Done() <-chan struct{} {
	return r.done
}

// Stop requests cancellation for the run.
func (r *Run[Value]) Stop() {
	r.cancel()
}

// Err returns the first task error or context error recorded by the run.
func (r *Run[Value]) Err() error {
	<-r.done
	r.errMu.Lock()
	defer r.errMu.Unlock()
	return r.err
}

// Submitted returns the number of tasks accepted by the dispatcher.
func (r *Run[Value]) Submitted() int64 {
	return r.submitted.Load()
}

// Completed returns the number of task results emitted by the dispatcher.
func (r *Run[Value]) Completed() int64 {
	return r.completed.Load()
}

func (r *Run[Value]) setErr(err error) {
	if err == nil {
		return
	}
	r.errMu.Lock()
	defer r.errMu.Unlock()
	if r.err == nil {
		r.err = err
	}
}

func failedRun[Value any](err error) *Run[Value] {
	results := make(chan Result[Value])
	done := make(chan struct{})
	close(results)
	close(done)
	_, cancel := context.WithCancel(context.Background())
	cancel()
	run := &Run[Value]{results: results, done: done, cancel: cancel}
	run.setErr(err)
	return run
}
