package celestial

import "errors"

// BatchSlice groups a slice into non-copying batches.
func BatchSlice[Task any](tasks []Task, batchSize int) ([][]Task, error) {
	if batchSize < 1 {
		return nil, errors.New("celestial: batch size must be positive")
	}
	if len(tasks) == 0 {
		return [][]Task{}, nil
	}

	batches := make([][]Task, 0, (len(tasks)+batchSize-1)/batchSize)
	for start := 0; start < len(tasks); start += batchSize {
		end := start + batchSize
		if end > len(tasks) {
			end = len(tasks)
		}
		batches = append(batches, tasks[start:end])
	}
	return batches, nil
}
