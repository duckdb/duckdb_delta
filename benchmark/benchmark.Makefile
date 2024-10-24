.PHONY: bench-output-dir clean_benchmark plot

ifeq ("$(BENCHMARK_PATTERN)a", "a")
    BENCHMARK_PATTERN:=.*
endif

ifeq ("$(IO_MODE)a", "a")
    IO_MODE:=local
endif

bench-output-dir:
	mkdir -p benchmark_results

clean_benchmark:
	rm -rf benchmark_results

plot:
	python3 scripts/plot.py

############### BENCHMARK TARGETS ###############

###
# TPCH
###

# TPCH SF1 on delta table
bench-run-tpch-sf1-delta: bench-output-dir
	./build/release/benchmark/benchmark_runner --root-dir './' 'benchmark/tpch/sf1/local/delta/$(BENCHMARK_PATTERN)' 2>&1 | tee benchmark_results/tpch-sf1-delta.csv
bench-run-tpch-sf1-delta-attach: bench-output-dir
	./build/release/benchmark/benchmark_runner --root-dir './' 'benchmark/tpch/sf1/local/delta_attach/$(BENCHMARK_PATTERN)' 2>&1 | tee benchmark_results/tpch-sf1-delta-attach.csv
# TPCH SF1 on parquet files
bench-run-tpch-sf1-parquet: bench-output-dir
	./build/release/benchmark/benchmark_runner 'benchmark/tpch/sf1-parquet/$(BENCHMARK_PATTERN)' 2>&1 | tee benchmark_results/tpch-sf1-parquet.csv
# TPCH SF1 on duckdb file
bench-run-tpch-sf1-duckdb: bench-output-dir
	./build/release/benchmark/benchmark_runner --root-dir './' 'benchmark/tpch/sf1/local/duckdb/$(BENCHMARK_PATTERN)' 2>&1 | tee benchmark_results/tpch-sf1-duckdb.csv
# COMPARES TPCH SF1 on parquet file vs on delta files vs on duckdb files
bench-run-tpch-sf1: bench-run-tpch-sf1-delta bench-run-tpch-sf1-parquet bench-run-tpch-sf1-attach

###
# TPCDS
###

# TPCDS SF1 on delta table
bench-run-tpcds-sf1-delta: bench-output-dir
	./build/release/benchmark/benchmark_runner --root-dir './' 'benchmark/tpcds/sf1/$(IO_MODE)/delta/$(BENCHMARK_PATTERN)' 2>&1 | tee benchmark_results/tpcds-sf1-delta-$(IO_MODE).csv
bench-run-tpcds-sf1-delta-attach: bench-output-dir
	./build/release/benchmark/benchmark_runner --root-dir './' 'benchmark/tpcds/sf1/$(IO_MODE)/delta_attach/$(BENCHMARK_PATTERN)' 2>&1 | tee benchmark_results/tpcds-sf1-delta-attach-$(IO_MODE).csv
bench-run-tpcds-sf1-delta-attach-pin: bench-output-dir
	./build/release/benchmark/benchmark_runner --root-dir './' 'benchmark/tpcds/sf1/$(IO_MODE)/delta_attach_pin/$(BENCHMARK_PATTERN)' 2>&1 | tee benchmark_results/tpcds-sf1-delta-attach-pin-$(IO_MODE).csv
# TPCDS SF1 on parquet files
bench-run-tpcds-sf1-parquet: bench-output-dir
	./build/release/benchmark/benchmark_runner --root-dir './' 'benchmark/tpcds/sf1/$(IO_MODE)/parquet/$(BENCHMARK_PATTERN)' 2>&1 | tee benchmark_results/tpcds-sf1-parquet-$(IO_MODE).csv
# TPCDS SF1 on duckdb files
bench-run-tpcds-sf1-duckdb: bench-output-dir
	./build/release/benchmark/benchmark_runner --root-dir './' 'benchmark/tpcds/sf1/$(IO_MODE)/duckdb/$(BENCHMARK_PATTERN)' 2>&1 | tee benchmark_results/tpcds-sf1-duckdb-$(IO_MODE).csv

# COMPARES TPCDS SF1 on parquet file vs on delta files
bench-run-tpcds-sf1: bench-run-tpcds-sf1-delta bench-run-tpcds-sf1-parquet bench-run-tpcds-sf1-duckdb bench-run-tpcds-sf1-delta-attach bench-run-tpcds-sf1-delta-attach-pin

###
# MICRO
###

bench-run-snapshot-performance: bench-output-dir
	./build/release/benchmark/benchmark_runner --root-dir './' 'benchmark/micro/snapshot_performance/.*' 2>&1 | tee benchmark_results/snapshot-performance.csv
