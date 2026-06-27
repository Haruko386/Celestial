package celestial

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"
)

func TestDispatcherRunSlice(t *testing.T) {
	dispatcher := New[int, int](Config{Workers: 3})
	run := dispatcher.RunSlice(context.Background(), []int{1, 2, 3, 4}, func(ctx context.Context, worker Worker, task int) (int, error) {
		return task * task, nil
	})

	var values []int
	for result := range run.Results() {
		if result.Err != nil {
			t.Fatalf("unexpected task error: %v", result.Err)
		}
		values = append(values, result.Value)
	}
	if err := run.Err(); err != nil {
		t.Fatalf("unexpected run error: %v", err)
	}

	sort.Ints(values)
	want := []int{1, 4, 9, 16}
	for i := range want {
		if values[i] != want[i] {
			t.Fatalf("values[%d] = %d, want %d", i, values[i], want[i])
		}
	}
	if run.Submitted() != 4 || run.Completed() != 4 {
		t.Fatalf("submitted/completed = %d/%d, want 4/4", run.Submitted(), run.Completed())
	}
}

func TestDispatcherRunSliceWithExecutor(t *testing.T) {
	dispatcher := New[int, int](Config{Workers: 2})
	run := dispatcher.RunSliceWith(context.Background(), []int{1, 2, 3}, multiplyExecutor{factor: 3})

	var sum int
	for result := range run.Results() {
		if result.Err != nil {
			t.Fatalf("unexpected task error: %v", result.Err)
		}
		sum += result.Value
	}
	if err := run.Err(); err != nil {
		t.Fatalf("unexpected run error: %v", err)
	}
	if sum != 18 {
		t.Fatalf("sum = %d, want 18", sum)
	}
}

func TestDispatcherStopOnError(t *testing.T) {
	wantErr := errors.New("boom")
	dispatcher := New[int, int](Config{Workers: 1, StopOnError: true})
	run := dispatcher.RunSlice(context.Background(), []int{1, 2, 3}, func(ctx context.Context, worker Worker, task int) (int, error) {
		if task == 2 {
			return 0, wantErr
		}
		return task, nil
	})

	for range run.Results() {
	}
	if !errors.Is(run.Err(), wantErr) {
		t.Fatalf("run.Err() = %v, want %v", run.Err(), wantErr)
	}
}

type multiplyExecutor struct {
	factor int
}

func (e multiplyExecutor) Execute(ctx context.Context, worker Worker, task int) (int, error) {
	return task * e.factor, nil
}

func TestDispatcherStopDoesNotWaitForTaskChannelClose(t *testing.T) {
	dispatcher := New[int, int](Config{Workers: 1})
	tasks := make(chan int)
	run := dispatcher.Run(context.Background(), tasks, func(ctx context.Context, worker Worker, task int) (int, error) {
		return task, nil
	})

	run.Stop()

	select {
	case <-run.Done():
	case <-time.After(time.Second):
		t.Fatal("run did not stop when the task channel stayed open")
	}
}

func TestSplitRange(t *testing.T) {
	ranges, err := SplitRange(1, 10, 4)
	if err != nil {
		t.Fatalf("SplitRange returned error: %v", err)
	}
	want := []Range{{Start: 1, End: 4}, {Start: 5, End: 8}, {Start: 9, End: 10}}
	if len(ranges) != len(want) {
		t.Fatalf("len(ranges) = %d, want %d", len(ranges), len(want))
	}
	for i := range want {
		if ranges[i] != want[i] {
			t.Fatalf("ranges[%d] = %+v, want %+v", i, ranges[i], want[i])
		}
	}
}
