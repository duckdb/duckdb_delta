# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(deltatable
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)

# Any extra extensions that should be built
duckdb_extension_load(httpfs)
#duckdb_extension_load(aws
#        LOAD_TESTS
#        GIT_URL https://github.com/duckdb/duckdb_aws
#        GIT_TAG f7b8729f1cce5ada5d4add70e1486de50763fb97
#        APPLY_PATCHES
#        )