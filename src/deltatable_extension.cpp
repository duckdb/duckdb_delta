#define DUCKDB_EXTENSION_MAIN

#include "deltatable_extension.hpp"
#include "deltatable_functions.hpp"

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/extension_util.hpp"

#include "parquet_reader.hpp"

namespace duckdb {

static void LoadInternal(DatabaseInstance &instance) {
    // Load functions
    for (const auto &function : DeltatableFunctions::GetTableFunctions(instance)) {
        ExtensionUtil::RegisterFunction(instance, function);
    }
}

void DeltatableExtension::Load(DuckDB &db) {
    LoadInternal(*db.instance);
}

std::string DeltatableExtension::Name() {
    return "deltatable";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void deltatable_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::DeltatableExtension>();
}

DUCKDB_EXTENSION_API const char *deltatable_version() {
    return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
