#include "storage/delta_transaction_manager.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

DeltaTransactionManager::DeltaTransactionManager(AttachedDatabase &db_p, DeltaCatalog &delta_catalog)
    : TransactionManager(db_p), delta_catalog(delta_catalog) {
}

Transaction &DeltaTransactionManager::StartTransaction(ClientContext &context) {
	auto transaction = make_uniq<DeltaTransaction>(delta_catalog, *this, context);
	transaction->Start();
	auto &result = *transaction;
	lock_guard<mutex> l(transaction_lock);
	transactions[result] = std::move(transaction);
	return result;
}

ErrorData DeltaTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	auto &delta_transaction = transaction.Cast<DeltaTransaction>();
	delta_transaction.Commit();
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
	return ErrorData();
}

void DeltaTransactionManager::RollbackTransaction(Transaction &transaction) {
	auto &delta_transaction = transaction.Cast<DeltaTransaction>();
	delta_transaction.Rollback();
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
}

void DeltaTransactionManager::Checkpoint(ClientContext &context, bool force) {
	// NOP
}

} // namespace duckdb
