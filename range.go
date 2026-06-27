package celestial

import (
	"context"
	"errors"
	"math"
)

// Range describes an inclusive int64 task range.
type Range struct {
	Start int64
	End   int64
}

// Len returns the number of values covered by the inclusive range.
func (r Range) Len() int64 {
	if r.End < r.Start {
		return 0
	}
	return r.End - r.Start + 1
}

// SplitRange splits an inclusive range into inclusive chunks.
func SplitRange(start int64, end int64, chunkSize int64) ([]Range, error) {
	if end < start {
		return nil, errors.New("celestial: range end is smaller than start")
	}
	if chunkSize < 1 {
		return nil, errors.New("celestial: chunk size must be positive")
	}

	ranges := make([]Range, 0, ((end-start)+chunkSize)/chunkSize)
	for current := start; current <= end; current += chunkSize {
		next := current + chunkSize - 1
		if next > end || next < current {
			next = end
		}
		ranges = append(ranges, Range{Start: current, End: next})
		if next == math.MaxInt64 {
			break
		}
	}
	return ranges, nil
}

// StreamRange streams SplitRange output without allocating a full slice.
func StreamRange(ctx context.Context, start int64, end int64, chunkSize int64) (<-chan Range, <-chan error) {
	out := make(chan Range)
	errs := make(chan error, 1)

	go func() {
		defer close(out)
		defer close(errs)

		if end < start {
			errs <- errors.New("celestial: range end is smaller than start")
			return
		}
		if chunkSize < 1 {
			errs <- errors.New("celestial: chunk size must be positive")
			return
		}

		for current := start; current <= end; current += chunkSize {
			next := current + chunkSize - 1
			if next > end || next < current {
				next = end
			}
			select {
			case out <- Range{Start: current, End: next}:
			case <-ctx.Done():
				errs <- ctx.Err()
				return
			}
			if next == math.MaxInt64 {
				return
			}
		}
	}()

	return out, errs
}
