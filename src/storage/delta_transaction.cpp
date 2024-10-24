#include "storage/delta_transaction.hpp"
#include "storage/delta_catalog.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "functions/delta_scan.hpp"
#include "storage/delta_table_entry.hpp"

namespace duckdb {

DeltaTransaction::DeltaTransaction(DeltaCatalog &delta_catalog, TransactionManager &manager, ClientContext &context)
    : Transaction(manager, context), access_mode(delta_catalog.access_mode) {
}

DeltaTransaction::~DeltaTransaction() {
}

void DeltaTransaction::Start() {
	transaction_state = DeltaTransactionState::TRANSACTION_NOT_YET_STARTED;
}
void DeltaTransaction::Commit() {
	if (transaction_state == DeltaTransactionState::TRANSACTION_STARTED) {
		transaction_state = DeltaTransactionState::TRANSACTION_FINISHED;
		// NOP: we only support read-only transactions currently
	}
}
void DeltaTransaction::Rollback() {
	if (transaction_state == DeltaTransactionState::TRANSACTION_STARTED) {
		transaction_state = DeltaTransactionState::TRANSACTION_FINISHED;
	    // NOP: we only support read-only transactions currently
	}
}

DeltaTransaction &DeltaTransaction::Get(ClientContext &context, Catalog &catalog) {
	return Transaction::Get(context, catalog).Cast<DeltaTransaction>();
}

AccessMode DeltaTransaction::GetAccessMode() const {
    return access_mode;
}

} // namespace duckdb
