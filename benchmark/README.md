# Benchmarking the Delta Extension

## Basics
A primitive benchmarking suite exists for the Delta extension.

To run the benchmarks, firstly run the build using:
```shell
BUILD_BENCHMARK=1 make
```

Then, make sure that the generated data is created using:
```shell
make generate-data
```

Then to run a benchmark, use one of the benchmark Makefile targets prefixed with `bench-run-`:
```shell
make bench-run-tpch-sf1
```
Now the TPCH benchmark will be run twice, once on parquet files and once on a delta table.

To create a plot from the results run:
```shell
make plot
```

## More options
Specific benchmarks can be run from a suite using the `BENCHMARK_PATTERN` variable. For example to compare
only Q01 from TPCH SF1, run:
```shell
BENCHMARK_PATTERN=q01.benchmark make bench-run-tpch-sf1
```

Also, we can run all local benchmarks using:
```shell
make  bench-run-all-local
```
Or all remote benchmarks using
```shell
make bench-run-all-remote
```