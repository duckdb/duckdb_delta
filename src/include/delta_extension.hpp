#pragma once

#include "duckdb.hpp"

#define DEFAULT_DELTA_TABLE "delta_table"

namespace duckdb {

class DeltaExtension : public Extension {
public:
	void Load(DuckDB &db) override;
	std::string Name() override;
};

} // namespace duckdb
