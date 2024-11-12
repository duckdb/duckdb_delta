#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/function/built_in_functions.hpp>

#include "duckdb/function/scalar_function.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

#include "delta_utils.hpp"
#include "delta_functions.hpp"

namespace duckdb {

static void GetDeltaTestExpression(DataChunk &input, ExpressionState &state, Vector &output) {
	output.SetVectorType(VectorType::CONSTANT_VECTOR);

	auto test_expression = ffi::get_testing_kernel_expression();
	ExpressionVisitor visitor;

	auto result = visitor.VisitKernelExpression(&test_expression);
	if (result->size() != 1) {
		throw InternalException("Unexpected result: expected single expression");
	}

	auto &expr = result->back();
	if (expr->GetExpressionType() != ExpressionType::CONJUNCTION_AND) {
		throw InternalException("Unexpected result:  expected single top level Conjuntion");
	}

	vector<Value> result_to_string;
	for (auto &expr : expr->Cast<ConjunctionExpression>().children) {
		result_to_string.push_back(expr->ToString());
	}

	output.SetValue(0, Value::LIST(result_to_string));
};

ScalarFunctionSet DeltaFunctions::GetExpressionFunction(DatabaseInstance &instance) {
	ScalarFunctionSet result;
	result.name = "get_delta_test_expression";

	ScalarFunction getvar({}, LogicalType::LIST(LogicalType::VARCHAR), GetDeltaTestExpression, nullptr, nullptr);
	result.AddFunction(getvar);

	return result;
}

} // namespace duckdb