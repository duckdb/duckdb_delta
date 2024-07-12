# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(delta
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)

# Build the httpfs extension to test with s3/http
duckdb_extension_load(httpfs)

# Build the azure extension to test with azure
duckdb_extension_load(azure
        LOAD_TESTS
        GIT_URL https://github.com/duckdb/duckdb_azure
        GIT_TAG 49b63dc8cd166952a0a34dfd54e6cfe5b823e05e
)

# Build the aws extension to test with credential providers
duckdb_extension_load(aws
        LOAD_TESTS
        GIT_URL https://github.com/duckdb/duckdb_aws
        GIT_TAG 3d1f5c8d0127ff7aaf127935721b197e5fdd95e5
)

# Build the tpch and tpcds extension for testing/benchmarking
duckdb_extension_load(tpch)
duckdb_extension_load(tpcds)
