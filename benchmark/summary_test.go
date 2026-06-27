package benchmark

import (
	"fmt"
	"math"
	"os"
	"sort"
	"sync"
	"testing"
	"text/tabwriter"
	"time"
)

type summaryRow struct {
	name        string
	workload    string
	workers     int
	batchSize   int
	tasks       int
	nsPerTask   float64
	tasksPerSec float64
	speedup     float64
	efficiency  float64
}

var (
	summaryMu   sync.Mutex
	summaryRows = make(map[string]summaryRow)
)

func TestMain(m *testing.M) {
	code := m.Run()
	printBenchmarkSummary()
	os.Exit(code)
}

func recordThroughput(b *testing.B, workload string, workers int, batchSize int, tasks int) {
	elapsed := b.Elapsed()
	if tasks < 1 || elapsed <= 0 {
		return
	}

	recordSummary(summaryRow{
		name:        b.Name(),
		workload:    workload,
		workers:     workers,
		batchSize:   batchSize,
		tasks:       tasks,
		nsPerTask:   nsPerTask(elapsed, tasks),
		tasksPerSec: float64(tasks) / elapsed.Seconds(),
		speedup:     math.NaN(),
		efficiency:  math.NaN(),
	})
}

func recordScore(b *testing.B, workload string, workers int, batchSize int, tasks int, speedup float64, efficiency float64) {
	recordSummary(summaryRow{
		name:        b.Name(),
		workload:    workload,
		workers:     workers,
		batchSize:   batchSize,
		tasks:       tasks,
		nsPerTask:   math.NaN(),
		tasksPerSec: math.NaN(),
		speedup:     speedup,
		efficiency:  efficiency,
	})
}

func recordSummary(row summaryRow) {
	summaryMu.Lock()
	defer summaryMu.Unlock()
	summaryRows[row.name] = row
}

func printBenchmarkSummary() {
	summaryMu.Lock()
	rows := make([]summaryRow, 0, len(summaryRows))
	for _, row := range summaryRows {
		rows = append(rows, row)
	}
	summaryMu.Unlock()

	if len(rows) == 0 {
		return
	}

	sort.Slice(rows, func(i int, j int) bool {
		return rows[i].name < rows[j].name
	})

	fmt.Fprintln(os.Stdout)
	fmt.Fprintln(os.Stdout, "Celestial benchmark summary")

	writer := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(writer, "Benchmark\tWorkload\tWorkers\tBatch\tTasks\tNs/task\tTasks/s\tSpeedup\tEfficiency")
	for _, row := range rows {
		fmt.Fprintf(
			writer,
			"%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			row.name,
			formatValue(row.workload),
			formatInt(row.workers),
			formatBatchSize(row.batchSize),
			formatInt(row.tasks),
			formatFloat(row.nsPerTask, "%.2f"),
			formatRate(row.tasksPerSec),
			formatFloat(row.speedup, "%.2fx"),
			formatFloat(row.efficiency, "%.2f"),
		)
	}
	_ = writer.Flush()
}

func nsPerTask(elapsed time.Duration, tasks int) float64 {
	return float64(elapsed.Nanoseconds()) / float64(tasks)
}

func formatValue(value string) string {
	if value == "" {
		return "-"
	}
	return value
}

func formatInt(value int) string {
	if value < 1 {
		return "-"
	}
	return fmt.Sprintf("%d", value)
}

func formatBatchSize(value int) string {
	if value < 1 {
		return "-"
	}
	return fmt.Sprintf("%d", value)
}

func formatFloat(value float64, format string) string {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return "-"
	}
	return fmt.Sprintf(format, value)
}

func formatRate(value float64) string {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return "-"
	}
	switch {
	case value >= 1_000_000_000:
		return fmt.Sprintf("%.2fB", value/1_000_000_000)
	case value >= 1_000_000:
		return fmt.Sprintf("%.2fM", value/1_000_000)
	case value >= 1_000:
		return fmt.Sprintf("%.2fK", value/1_000)
	default:
		return fmt.Sprintf("%.2f", value)
	}
}
