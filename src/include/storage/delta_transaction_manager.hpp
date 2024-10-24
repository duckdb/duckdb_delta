//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/delta_transaction_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction_manager.hpp"
#include "storage/delta_catalog.hpp"
#include "storage/delta_transaction.hpp"

namespace duckdb {

class DeltaTransactionManager : public TransactionManager {
public:
	DeltaTransactionManager(AttachedDatabase &db_p, DeltaCatalog &delta_catalog);

	Transaction &StartTransaction(ClientContext &context) override;
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	void RollbackTransaction(Transaction &transaction) override;

	void Checkpoint(ClientContext &context, bool force = false) override;

private:
	DeltaCatalog &delta_catalog;
	mutex transaction_lock;
	reference_map_t<Transaction, unique_ptr<DeltaTransaction>> transactions;
};

} // namespace duckdb
