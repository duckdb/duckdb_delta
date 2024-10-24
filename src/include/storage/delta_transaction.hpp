//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/delta_transaction.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction.hpp"

namespace duckdb {
class DeltaCatalog;
class DeltaSchemaEntry;
class DeltaTableEntry;
struct DeltaSnapshot;

enum class DeltaTransactionState { TRANSACTION_NOT_YET_STARTED, TRANSACTION_STARTED, TRANSACTION_FINISHED };

class DeltaTransaction : public Transaction {
public:
	DeltaTransaction(DeltaCatalog &delta_catalog, TransactionManager &manager, ClientContext &context);
	~DeltaTransaction() override;

	void Start();
	void Commit();
	void Rollback();

	static DeltaTransaction &Get(ClientContext &context, Catalog &catalog);
	AccessMode GetAccessMode() const;

    void SetReadWrite() override {
        throw NotImplementedException("Can not start read-write transaction");
    };
public:
    unique_ptr<DeltaTableEntry> table_entry;

private:
	//	DeltaConnection connection;
	DeltaTransactionState transaction_state;
	AccessMode access_mode;
};

} // namespace duckdb
