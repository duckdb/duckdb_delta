//===----------------------------------------------------------------------===//
//                         DuckDB
//
// deltatable_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

namespace duckdb {

class DeltatableFunctions {
public:
    static vector<TableFunctionSet> GetTableFunctions();

private:
    static TableFunctionSet GetDeltaScanFunction();
};
} // namespace duckdb