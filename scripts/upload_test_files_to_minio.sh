#!/bin/bash

aws s3 cp --endpoint-url http://duckdb-minio.com:9000 --recursive ./build/release/rust/src/delta_kernel/acceptance/tests/dat/out/reader_tests/generated "s3://test-bucket/dat"
aws s3 cp --endpoint-url http://duckdb-minio.com:9000 --recursive ./build/release/rust/src/delta_kernel/acceptance/tests/dat/out/reader_tests/generated "s3://test-bucket-public/dat"