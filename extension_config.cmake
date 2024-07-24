# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(delta
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)

# Build the httpfs extension to test with s3/http
duckdb_extension_load(httpfs)

## Build the azure extension to test with azure
#duckdb_extension_load(azure
#        LOAD_TESTS
#        GIT_URL https://github.com/duckdb/duckdb_azure
#        GIT_TAG d92b3b87ff06e6694883b1a6dbf684eeefedd609
#)
#
## Build the aws extension to test with credential providers
#duckdb_extension_load(aws
#        LOAD_TESTS
#        GIT_URL https://github.com/duckdb/duckdb_aws
#        GIT_TAG 42c78d3f99e1a188a2b178ea59e3c17907af4fb2
#)

# Build the tpch and tpcds extension for testing/benchmarking
duckdb_extension_load(tpch)
duckdb_extension_load(tpcds)
