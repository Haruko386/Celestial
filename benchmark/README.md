# Dispatcher Benchmark

This folder contains a small benchmark suite for evaluating task dispatch performance.

It is designed to answer three questions:

- How much overhead does the dispatcher add per task?
- How much throughput does it reach with different worker counts?
- How efficient is the worker scaling compared with a sequential baseline?

## Run

```bash
go test ./benchmark -bench . -benchmem -count 5
```

Run with specific CPU limits:

```bash
go test ./benchmark -bench . -benchmem -count 5 -cpu 1,2,4,8
```

Run one workload:

```bash
go test ./benchmark -bench BenchmarkChannelDispatcher/CPUWork -benchmem -count 5
```

Run the score benchmark:

```bash
go test ./benchmark -bench BenchmarkEvaluationScore -benchmem -count 5
```

## Benchmarks

- `BenchmarkSequential`: single-thread baseline.
- `BenchmarkChannelDispatcher`: channel-based worker-pool dispatch.
- `BenchmarkAtomicSliceDispatcher`: atomic index dispatch for in-memory slices.
- `BenchmarkBatchDispatcher`: batch dispatch for very small tasks.
- `BenchmarkEvaluationScore`: direct speedup and efficiency score.

## Workloads

- `TinyWork`: almost no work. It mostly measures dispatch overhead.
- `CPUWork`: deterministic CPU work. It measures scaling under real computation.

## Metrics

Go reports:

- `ns/op`: average time per task.
- `B/op`: allocated bytes per task.
- `allocs/op`: allocations per task.

The suite also reports custom Go benchmark metrics:

- `tasks/s`: completed tasks per second.
- `workers`: worker count used by the dispatcher.
- `batch_size`: batch size for batch benchmarks.
- `speedup`: sequential duration divided by dispatcher duration.
- `efficiency`: speedup divided by worker count.

After benchmarks finish, the CLI prints a `Celestial benchmark summary` table with:

- benchmark name
- workload
- Go runtime `GOMAXPROCS` value
- worker count
- batch size
- average task count
- sample count
- average ns/task
- average tasks/s
- average speedup
- average efficiency

Rows are grouped by benchmark name and `GOMAXPROCS`, so `-count 5` reports one averaged summary row with `Samples` set to `5`.

`GoProcs` and `Workers` are intentionally different for dispatcher benchmarks. Go's benchmark suffix, such as `-16`, means `GOMAXPROCS=16`; the benchmark dispatcher uses `GOMAXPROCS - 1` workers when possible, leaving one CPU for coordination and runtime overhead.

## Evaluation

Use these formulas when comparing results:

```text
speedup = sequential ns/op / dispatcher ns/op
efficiency = speedup / workers
```

`BenchmarkEvaluationScore` reports these values directly, and the final summary table reports their averages across repeated runs.

Suggested interpretation:

- Tiny tasks should have low `ns/op` and low allocations.
- CPU tasks should show higher `tasks/s` as workers increase.
- Efficiency close to `1.0` means worker scaling is strong.
- Low efficiency on tiny tasks usually means dispatch overhead dominates.

## Notes

Benchmark results depend on CPU model, power settings, background load, and Go version. Use `-count 5` or higher and compare the final summary averages or the raw run-to-run spread.
