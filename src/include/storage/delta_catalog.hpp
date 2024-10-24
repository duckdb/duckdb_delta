//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/delta_catalog.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "delta_schema_entry.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/enums/access_mode.hpp"

namespace duckdb {
class DeltaSchemaEntry;

class DeltaClearCacheFunction : public TableFunction {
public:
	DeltaClearCacheFunction();

	static void ClearCacheOnSetting(ClientContext &context, SetScope scope, Value &parameter);
};

class DeltaCatalog : public Catalog {
public:
	explicit DeltaCatalog(AttachedDatabase &db_p, const string &internal_name, AccessMode access_mode);
	~DeltaCatalog();

	string path;
	AccessMode access_mode;
    bool use_cache;

public:
	void Initialize(bool load_builtin) override;
	string GetCatalogType() override {
		return "delta";
	}

	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;

	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;

	optional_ptr<SchemaCatalogEntry> GetSchema(CatalogTransaction transaction, const string &schema_name,
	                                           OnEntryNotFound if_not_found,
	                                           QueryErrorContext error_context = QueryErrorContext()) override;

	unique_ptr<PhysicalOperator> PlanInsert(ClientContext &context, LogicalInsert &op,
	                                        unique_ptr<PhysicalOperator> plan) override;
	unique_ptr<PhysicalOperator> PlanCreateTableAs(ClientContext &context, LogicalCreateTable &op,
	                                               unique_ptr<PhysicalOperator> plan) override;
	unique_ptr<PhysicalOperator> PlanDelete(ClientContext &context, LogicalDelete &op,
	                                        unique_ptr<PhysicalOperator> plan) override;
	unique_ptr<PhysicalOperator> PlanUpdate(ClientContext &context, LogicalUpdate &op,
	                                        unique_ptr<PhysicalOperator> plan) override;
	unique_ptr<LogicalOperator> BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table,
	                                            unique_ptr<LogicalOperator> plan) override;

	DatabaseSize GetDatabaseSize(ClientContext &context) override;

    optional_idx GetCatalogVersion(ClientContext &context) override;

	bool InMemory() override;
	string GetDBPath() override;

    bool UseCachedSnapshot();

    DeltaSchemaEntry& GetMainSchema() {
        return *main_schema;
    }

private:
	void DropSchema(ClientContext &context, DropInfo &info) override;

private:
    unique_ptr<DeltaSchemaEntry> main_schema;
	string default_schema;
};

} // namespace duckdb
