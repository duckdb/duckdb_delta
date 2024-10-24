#include "storage/delta_catalog.hpp"
#include "storage/delta_schema_entry.hpp"
#include "storage/delta_transaction.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/main/attached_database.hpp"

#include "functions/delta_scan.hpp"
#include "storage/delta_transaction_manager.hpp"

namespace duckdb {

DeltaCatalog::DeltaCatalog(AttachedDatabase &db_p, const string &path, AccessMode access_mode)
    : Catalog(db_p), path(path), access_mode(access_mode), use_cache(false) {
}

DeltaCatalog::~DeltaCatalog() = default;

void DeltaCatalog::Initialize(bool load_builtin) {
    CreateSchemaInfo info;
    main_schema = make_uniq<DeltaSchemaEntry>(*this, info);
}

optional_ptr<CatalogEntry> DeltaCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
    throw BinderException("Delta tables do not support creating new schemas");
}

void DeltaCatalog::DropSchema(ClientContext &context, DropInfo &info) {
    throw BinderException("Delta tables do not support dropping schemas");
}

void DeltaCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
    callback(*main_schema);
}

optional_ptr<SchemaCatalogEntry> DeltaCatalog::GetSchema(CatalogTransaction transaction, const string &schema_name,
                                                      OnEntryNotFound if_not_found, QueryErrorContext error_context) {
    if (schema_name == DEFAULT_SCHEMA || schema_name == INVALID_SCHEMA) {
        return main_schema.get();
    }
    if (if_not_found == OnEntryNotFound::RETURN_NULL) {
        return nullptr;
    }
    return nullptr;
}

bool DeltaCatalog::InMemory() {
	return false;
}

string DeltaCatalog::GetDBPath() {
	return path;
}

bool DeltaCatalog::UseCachedSnapshot() {
    return use_cache;
}

optional_idx DeltaCatalog::GetCatalogVersion(ClientContext &context) {
    auto &delta_transaction = DeltaTransaction::Get(context, *this);

    // Option 1: snapshot is cached table-wide
    auto cached_snapshot = main_schema->GetCachedTable();
    if (cached_snapshot) {
        return cached_snapshot->snapshot->version;
    }

    // Option 2: snapshot is cached in transaction
    if (delta_transaction.table_entry) {
        return delta_transaction.table_entry->snapshot->version;
    }

    // FIXME: this is not allowed
    return optional_idx::Invalid();
}

DatabaseSize DeltaCatalog::GetDatabaseSize(ClientContext &context) {
	if (default_schema.empty()) {
		throw InvalidInputException("Attempting to fetch the database size - but no database was provided "
		                            "in the connection string");
	}
	DatabaseSize size;
	return size;
}

unique_ptr<PhysicalOperator> DeltaCatalog::PlanInsert(ClientContext &context, LogicalInsert &op,
                                                   unique_ptr<PhysicalOperator> plan) {
	throw NotImplementedException("DeltaCatalog does not support inserts");
}
unique_ptr<PhysicalOperator> DeltaCatalog::PlanCreateTableAs(ClientContext &context, LogicalCreateTable &op,
                                                          unique_ptr<PhysicalOperator> plan) {
	throw NotImplementedException("DeltaCatalog does not support creating new tables");
}
unique_ptr<PhysicalOperator> DeltaCatalog::PlanDelete(ClientContext &context, LogicalDelete &op,
                                                   unique_ptr<PhysicalOperator> plan) {
	throw NotImplementedException("DeltaCatalog does not support deletes");
}
unique_ptr<PhysicalOperator> DeltaCatalog::PlanUpdate(ClientContext &context, LogicalUpdate &op,
                                                   unique_ptr<PhysicalOperator> plan) {
	throw NotImplementedException("DeltaCatalog does not support updates");
}
unique_ptr<LogicalOperator> DeltaCatalog::BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table,
                                                       unique_ptr<LogicalOperator> plan) {
	throw NotImplementedException("DeltaCatalog does not support creating indices");
}

} // namespace duckdb
