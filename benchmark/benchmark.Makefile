.PHONY: bench-output-dir clean_benchmark plot

ifeq ("$(BENCHMARK_PATTERN)a", "a")
    BENCHMARK_PATTERN:=.*
endif

bench-output-dir:
	mkdir -p benchmark_results

clean_benchmark:
	rm -rf benchmark_results

plot:
	python3 scripts/plot.py


############### BENCHMARK TARGETS ###############

###
# TPCH LOCAL
###

# TPCH SF1 on delta table
bench-run-tpch-sf1-delta: bench-output-dir
	./build/release/benchmark/benchmark_runner --root-dir './' 'benchmark/tpch/sf1-delta/$(BENCHMARK_PATTERN)' 2>&1 | tee benchmark_results/tpch-sf1-delta.csv
# TPCH SF1 on parquet files
bench-run-tpch-sf1-parquet: bench-output-dir
	./build/release/benchmark/benchmark_runner 'benchmark/tpch/sf1-parquet/$(BENCHMARK_PATTERN)' 2>&1 | tee benchmark_results/tpch-sf1-parquet.csv
# COMPARES TPCH SF1 on parquet file vs on delta files
bench-run-tpch-sf1: bench-run-tpch-sf1-delta bench-run-tpch-sf1-parquet

###
# TPCH REMOTE
###

# TPCH on remote delta table (set BENCHMARK_DATA_S3_LINEITEM_SF1)
bench-run-tpch-sf1-remote-delta: bench-output-dir
	./build/release/benchmark/benchmark_runner --root-dir './' 'benchmark/tpch/sf1-delta-remote/$(BENCHMARK_PATTERN)' 2>&1 | tee benchmark_results/tpch-sf1-remote-delta.csv
# TPCH on remote parquet table (set BENCHMARK_DATA_S3_LINEITEM_SF1)
bench-run-tpch-sf1-remote-parquet: bench-output-dir
	./build/release/benchmark/benchmark_runner --root-dir './' 'benchmark/tpch/sf1-parquet-remote/$(BENCHMARK_PATTERN)'  2>&1 | tee benchmark_results/tpch-sf1-remote-parquet.csv
# COMPARES TPCH SF1 on parquet file vs on delta files
bench-run-tpch-sf1-remote: bench-run-tpch-sf1-remote-parquet bench-run-tpch-sf1-remote-delta

###
# TPCDS LOCAL
###

# TPCDS SF1 on delta table
bench-run-tpcds-sf1-delta: bench-output-dir
	./build/release/benchmark/benchmark_runner --root-dir './' 'benchmark/tpcds/sf1-delta/$(BENCHMARK_PATTERN)' 2>&1 | tee benchmark_results/tpcds-sf1-delta.csv
# TPCDS SF1 on parquet files
bench-run-tpcds-sf1-parquet: bench-output-dir
	./build/release/benchmark/benchmark_runner --root-dir './' 'benchmark/tpcds/sf1-parquet/$(BENCHMARK_PATTERN)' 2>&1 | tee benchmark_results/tpcds-sf1-parquet.csv
# COMPARES TPCDS SF1 on parquet file vs on delta files
bench-run-tpcds-sf1: bench-run-tpcds-sf1-delta bench-run-tpcds-sf1-parquet

###
# ALL
###
bench-run-all-local: bench-run-tpcds-sf1 bench-run-tpch-sf1

bench-run-all-remote: bench-run-tpch-sf1-remote
