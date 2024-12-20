//===----------------------------------------------------------------------===//
//                         DuckDB
//
// delta_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

namespace duckdb {

class DeltaFunctions {
public:
	static vector<TableFunctionSet> GetTableFunctions(DatabaseInstance &instance);
	static vector<ScalarFunctionSet> GetScalarFunctions(DatabaseInstance &instance);

private:
	//! Table Functions
	static TableFunctionSet GetDeltaScanFunction(DatabaseInstance &instance);

	//! Scalar Functions
	static ScalarFunctionSet GetExpressionFunction(DatabaseInstance &instance);
};
} // namespace duckdb
