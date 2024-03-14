#include "deltatable_functions.hpp"

#include "duckdb.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

namespace duckdb {

vector<TableFunctionSet> DeltatableFunctions::GetTableFunctions() {
    vector<TableFunctionSet> functions;

    functions.push_back(GetDeltaScanFunction());

    return functions;
}

};
