#include "storage/delta_catalog.hpp"
#include "storage/delta_schema_entry.hpp"
#include "storage/delta_table_entry.hpp"

#include "delta_utils.hpp"
#include "functions/delta_scan.hpp"

#include "storage/delta_transaction.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "../../duckdb/third_party/catch/catch.hpp"
#include "functions/delta_scan.hpp"

#include <functional>

namespace duckdb {

DeltaTableEntry::DeltaTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info)
    : TableCatalogEntry(catalog, schema, info) {
	this->internal = false;
}

DeltaTableEntry::~DeltaTableEntry() = default;

unique_ptr<BaseStatistics> DeltaTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
	return nullptr;
}

void DeltaTableEntry::BindUpdateConstraints(Binder &binder, LogicalGet &, LogicalProjection &, LogicalUpdate &,
                                         ClientContext &) {
	throw NotImplementedException("BindUpdateConstraints for delta table");
}

TableFunction DeltaTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	auto &db = DatabaseInstance::GetDatabase(context);
	auto &delta_function_set = ExtensionUtil::GetTableFunction(db, "delta_scan");

	auto delta_scan_function = delta_function_set.functions.GetFunctionByArguments(context, {LogicalType::VARCHAR});
	auto &delta_catalog = catalog.Cast<DeltaCatalog>();

    // Copy over the internal kernel snapshot
    auto function_info = make_shared_ptr<DeltaFunctionInfo>();

    function_info->snapshot = this->snapshot;
    delta_scan_function.function_info = std::move(function_info);

	vector<Value> inputs = {delta_catalog.GetDBPath()};
	named_parameter_map_t param_map;
	vector<LogicalType> return_types;
	vector<string> names;
	TableFunctionRef empty_ref;


	TableFunctionBindInput bind_input(inputs, param_map, return_types, names, nullptr, nullptr, delta_scan_function,
	                                  empty_ref);

	auto result = delta_scan_function.bind(context, bind_input, return_types, names);
	bind_data = std::move(result);

	return delta_scan_function;
}

TableStorageInfo DeltaTableEntry::GetStorageInfo(ClientContext &context) {
	TableStorageInfo result;
	// TODO fill info
	return result;
}

} // namespace duckdb
