#define DUCKDB_EXTENSION_MAIN

#include "deltatable_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

namespace duckdb {

inline void DeltatableScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &name_vector = args.data[0];
    UnaryExecutor::Execute<string_t, string_t>(
	    name_vector, result, args.size(),
	    [&](string_t name) {
			return StringVector::AddString(result, "Deltatable "+name.GetString()+" 🐥");;
        });
}

static void LoadInternal(DatabaseInstance &instance) {
    // Register a scalar function
    auto deltatable_scalar_function = ScalarFunction("deltatable", {LogicalType::VARCHAR}, LogicalType::VARCHAR, DeltatableScalarFun);
    ExtensionUtil::RegisterFunction(instance, deltatable_scalar_function);
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
