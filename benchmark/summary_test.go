package benchmark

import (
	"fmt"
	"math"
	"os"
	"runtime"
	"sort"
	"sync"
	"testing"
	"text/tabwriter"
	"time"
)

type summaryRow struct {
	name        string
	workload    string
	goProcs     int
	workers     int
	batchSize   int
	tasks       int
	iterations  int
	nsPerTask   float64
	tasksPerSec float64
	speedup     float64
	efficiency  float64
	samples     int
}

type summaryAggregate struct {
	name      string
	workload  string
	goProcs   int
	workers   int
	batchSize int

	samples int
	tasks   metricAggregate

	iterations  metricAggregate
	nsPerTask   metricAggregate
	tasksPerSec metricAggregate
	speedup     metricAggregate
	efficiency  metricAggregate
}

type metricAggregate struct {
	sum   float64
	count int
}

var (
	summaryMu   sync.Mutex
	summaryRows []summaryRow
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
		goProcs:     runtime.GOMAXPROCS(0),
		workers:     workers,
		batchSize:   batchSize,
		tasks:       tasks,
		iterations:  b.N,
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
		goProcs:     runtime.GOMAXPROCS(0),
		workers:     workers,
		batchSize:   batchSize,
		tasks:       tasks,
		iterations:  b.N,
		nsPerTask:   math.NaN(),
		tasksPerSec: math.NaN(),
		speedup:     speedup,
		efficiency:  efficiency,
	})
}

func recordSummary(row summaryRow) {
	summaryMu.Lock()
	defer summaryMu.Unlock()
	summaryRows = append(summaryRows, row)
}

func printBenchmarkSummary() {
	summaryMu.Lock()
	records := append([]summaryRow(nil), summaryRows...)
	summaryMu.Unlock()

	if len(records) == 0 {
		return
	}

	rows := averageSummaryRows(records)
	sort.Slice(rows, func(i int, j int) bool {
		return rows[i].name < rows[j].name
	})

	fmt.Fprintln(os.Stdout)
	fmt.Fprintln(os.Stdout, "Celestial benchmark summary")

	writer := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(writer, "Benchmark\tWorkload\tGoProcs\tWorkers\tBatch\tAvgTasks\tSamples\tAvgNs/task\tAvgTasks/s\tAvgSpeedup\tAvgEfficiency")
	for _, row := range rows {
		fmt.Fprintf(
			writer,
			"%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			row.name,
			formatValue(row.workload),
			formatInt(row.goProcs),
			formatInt(row.workers),
			formatBatchSize(row.batchSize),
			formatInt(row.tasks),
			formatInt(row.samples),
			formatFloat(row.nsPerTask, "%.2f"),
			formatRate(row.tasksPerSec),
			formatFloat(row.speedup, "%.2fx"),
			formatFloat(row.efficiency, "%.2f"),
		)
	}
	_ = writer.Flush()
}

func (a *summaryAggregate) average() summaryRow {
	return summaryRow{
		name:        a.name,
		workload:    a.workload,
		goProcs:     a.goProcs,
		workers:     a.workers,
		batchSize:   a.batchSize,
		tasks:       int(math.Round(a.tasks.average())),
		iterations:  int(math.Round(a.iterations.average())),
		nsPerTask:   a.nsPerTask.average(),
		tasksPerSec: a.tasksPerSec.average(),
		speedup:     a.speedup.average(),
		efficiency:  a.efficiency.average(),
		samples:     a.samples,
	}
}

func averageSummaryRows(records []summaryRow) []summaryRow {
	maxIterations := make(map[string]int)
	for _, row := range records {
		key := summaryKey(row)
		if row.iterations > maxIterations[key] {
			maxIterations[key] = row.iterations
		}
	}

	aggregates := make(map[string]*summaryAggregate)
	for _, row := range records {
		maxIteration := maxIterations[summaryKey(row)]
		if maxIteration > 1 && row.iterations < maxIteration/2 {
			continue
		}

		key := summaryKey(row)
		aggregate := aggregates[key]
		if aggregate == nil {
			aggregate = &summaryAggregate{
				name:      row.name,
				workload:  row.workload,
				goProcs:   row.goProcs,
				workers:   row.workers,
				batchSize: row.batchSize,
			}
			aggregates[key] = aggregate
		}

		aggregate.samples++
		aggregate.tasks.add(float64(row.tasks))
		aggregate.iterations.add(float64(row.iterations))
		aggregate.nsPerTask.add(row.nsPerTask)
		aggregate.tasksPerSec.add(row.tasksPerSec)
		aggregate.speedup.add(row.speedup)
		aggregate.efficiency.add(row.efficiency)
	}

	rows := make([]summaryRow, 0, len(aggregates))
	for _, aggregate := range aggregates {
		rows = append(rows, aggregate.average())
	}
	return rows
}

func summaryKey(row summaryRow) string {
	return fmt.Sprintf("%s/%d", row.name, row.goProcs)
}

func (a *metricAggregate) add(value float64) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return
	}
	a.sum += value
	a.count++
}

func (a metricAggregate) average() float64 {
	if a.count == 0 {
		return math.NaN()
	}
	return a.sum / float64(a.count)
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
