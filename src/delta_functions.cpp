#include "delta_functions.hpp"

#include "duckdb.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

namespace duckdb {

vector<TableFunctionSet> DeltaFunctions::GetTableFunctions(DatabaseInstance &instance) {
	vector<TableFunctionSet> functions;

	functions.push_back(GetDeltaScanFunction(instance));

	return functions;
}

vector<ScalarFunctionSet> DeltaFunctions::GetScalarFunctions(DatabaseInstance &instance) {
	vector<ScalarFunctionSet> functions;

	functions.push_back(GetExpressionFunction(instance));

	return functions;
}

}; // namespace duckdb
