#pragma once

#include "duckdb.hpp"

namespace duckdb {

class ParquetOverrideFunction {
public:
    static TableFunctionSet GetFunctionSet();
};

} // namespace duckdb
