#define DUCKDB_EXTENSION_MAIN

#include "delta_extension.hpp"
#include "delta_functions.hpp"

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "storage/delta_catalog.hpp"
#include "storage/delta_transaction_manager.hpp"

namespace duckdb {

static unique_ptr<Catalog> DeltaCatalogAttach(StorageExtensionInfo *storage_info, ClientContext &context,
                                       AttachedDatabase &db, const string &name, AttachInfo &info,
                                       AccessMode access_mode) {

    auto res = make_uniq<DeltaCatalog>(db, info.path, access_mode);

    for (const auto& option : info.options) {
        if (StringUtil::Lower(option.first) == "pin_snapshot") {
            res->use_cache = option.second.GetValue<bool>();
        }
    }

    res->SetDefaultTable(DEFAULT_SCHEMA, DEFAULT_DELTA_TABLE);

	return std::move(res);
}

static unique_ptr<TransactionManager> CreateTransactionManager(StorageExtensionInfo *storage_info, AttachedDatabase &db,
                                                               Catalog &catalog) {
	auto &delta_catalog = catalog.Cast<DeltaCatalog>();
	return make_uniq<DeltaTransactionManager>(db, delta_catalog);
}

class DeltaStorageExtension : public StorageExtension {
public:
	DeltaStorageExtension() {
		attach = DeltaCatalogAttach;
		create_transaction_manager = CreateTransactionManager;
	}
};

static void LoadInternal(DatabaseInstance &instance) {
    // Load functions
    for (const auto &function : DeltaFunctions::GetTableFunctions(instance)) {
        ExtensionUtil::RegisterFunction(instance, function);
    }

    // Register the "single table" delta catalog (to ATTACH a single delta table)
    auto &config = DBConfig::GetConfig(instance);
    config.storage_extensions["delta"] = make_uniq<DeltaStorageExtension>();
}

void DeltaExtension::Load(DuckDB &db) {
    LoadInternal(*db.instance);
}

std::string DeltaExtension::Name() {
    return "delta";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void delta_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::DeltaExtension>();
}

DUCKDB_EXTENSION_API const char *delta_version() {
    return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
