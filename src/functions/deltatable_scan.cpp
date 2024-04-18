#include "duckdb/function/table_function.hpp"

#include "deltatable_functions.hpp"
#include "functions/deltatable_scan.hpp"
#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/binder.hpp"

#include <string>
#include <numeric>

namespace duckdb {

static void print_selection_vector(char* indent, const struct ffi::KernelBoolSlice *selection_vec) {
    for (int i = 0; i < selection_vec->len; i++) {
        printf("%ssel[%i] = %s\n", indent, i, selection_vec->ptr[i] ? "1" : "0");
    }
}

static void* allocate_string(const struct ffi::KernelStringSlice slice) {
    return new string(slice.ptr, slice.len);
}

static void visit_callback(void* engine_context, const struct ffi::KernelStringSlice path, int64_t size, ffi::CDvInfo *dv_info, struct ffi::CStringMap *partition_values) {
    auto context = (DeltaTableSnapshot *) engine_context;
    auto path_string =  context->path + "/" + from_delta_string_slice(path);

//    printf("Fetch metadata for %s\n", path_string.c_str());

    // First we append the file to our resolved files
    context->resolved_files.push_back(path_string);

    D_ASSERT(context->metadata.find(path_string) == context->metadata.end());

    // Initialize the file metadata
    context->metadata[path_string] = {};
    context->metadata[path_string].delta_snapshot_version = context->version;
    context->metadata[path_string].file_number = context->resolved_files.size() - 1;

    // Fetch the deletion vector
    ffi::KernelBoolSlice *selection_vector = ffi::selection_vector_from_dv(dv_info, context->table_client, context->global_state);
    if (selection_vector) {
        context->metadata[path_string].selection_vector = {selection_vector, ffi::drop_bool_slice};
    }

    // Lookup all columns for potential hits in the constant map
    case_insensitive_map_t<string> constant_map;
    for (const auto &col: context->names) {
        auto key = to_delta_string_slice(col);
        auto *partition_val = (string *) ffi::get_from_map(partition_values, key, allocate_string);
        if (partition_val) {
//            printf("- %s = %s\n", col.c_str(), (*partition_val).c_str());
            constant_map[col] = *partition_val;
            delete partition_val;
        }
    }
    context->metadata[path_string].partition_map = std::move(constant_map);
}

static void visit_data(void *engine_context, struct ffi::EngineDataHandle *engine_data, const struct ffi::KernelBoolSlice selection_vec) {
//    printf("Got some data\n");
//    printf("  Of this data, here is a selection vector\n");
//    print_selection_vector("    ", &selection_vec);
    ffi::visit_scan_data(engine_data, selection_vec, engine_context, visit_callback);
}

DeltaTableSnapshot::DeltaTableSnapshot(const string &path) : path(path) {
    auto path_slice = to_delta_string_slice(path);

    // Initialize Table Client
    auto table_client_res = ffi::get_default_client(path_slice, error_allocator);
    table_client = unpack_result_or_throw(table_client_res, "get_default_client in DeltaScanScanBind");

    // Initialize Snapshot
    auto snapshot_res = ffi::snapshot(path_slice, table_client);
    snapshot = unpack_result_or_throw(snapshot_res, "snapshot in DeltaScanScanBind");

    auto scan_res = ffi::scan(snapshot, table_client, nullptr);
    scan = unpack_result_or_throw(scan_res, "scan in DeltaScanScanBind");

    global_state = ffi::get_global_scan_state(scan);

    // Set version
    this->version = ffi::version(snapshot);
}

void DeltaTableSnapshot::Bind(vector<LogicalType> &return_types, vector<string> &names) {
    auto schema = SchemaVisitor::VisitSnapshotSchema(snapshot);
    for (const auto &field: *schema) {
        names.push_back(field.first);
        return_types.push_back(field.second);
    }
    // Store the bound names for resolving the complex filter pushdown later
    this->names = names;
}

string DeltaTableSnapshot::GetFile(idx_t i) {
    if (!initialized) {
        InitializeFiles();
    }
    // We already have this file
    if (i < resolved_files.size()) {
        return resolved_files[i];
    }

    if (i != resolved_files.size()) {
        throw InternalException("Calling GetFile on a file beyond the first unloaded file is not allowed!");
    }

    auto have_scan_data_res = ffi::kernel_scan_data_next(scan_data_iterator.get(), this, visit_data);
    auto have_scan_data = unpack_result_or_throw(have_scan_data_res, "kernel_scan_data_next in DeltaTableSnapshot GetFile");

    // TODO: shouldn't the kernel always return false here?
    if (!have_scan_data || resolved_files.size() == i) {
        resolved_files.push_back("");
    }

    // The kernel scan visitor should have resolved a file OR returned
    if(resolved_files.size() <= i) {
        throw InternalException("Delta Kernel seems to have failed to resolve a new file");
    }

    return resolved_files[i];
}

void DeltaTableSnapshot::InitializeFiles() {
    PredicateVisitor visitor(names, &table_filters);

    auto scan_iterator_res = ffi::kernel_scan_data_init(table_client, scan);
    scan_data_iterator = {
            unpack_result_or_throw(scan_iterator_res, "kernel_scan_data_init in InitFiles"),
            ffi::kernel_scan_data_free
    };
    initialized = true;
}

bool DeltaTableSnapshot::ComplexFilterPushdown(ClientContext &context, const MultiFileReaderOptions &options, LogicalGet &get,
                                               vector<unique_ptr<Expression>> &filters) {
    FilterCombiner combiner(context);
    for (const auto &filter : filters) {
        combiner.AddFilter(filter->Copy());
    }
    table_filters = combiner.GenerateTableScanFilters(get.column_ids);

    // TODO: can/should we figure out if this filtered anything?
    return true;
}

unique_ptr<MultiFileReader> DeltaMultiFileReader::CreateInstance() {
    return std::move(make_uniq<DeltaMultiFileReader>());
}

bool DeltaMultiFileReader::Bind(MultiFileReaderOptions &options, MultiFileList &files,
              vector<LogicalType> &return_types, vector<string> &names, MultiFileReaderBindData &bind_data)  {
    auto &delta_table_snapshot = dynamic_cast<DeltaTableSnapshot&>(files);

    delta_table_snapshot.Bind(return_types, names);

    return true;
};

void DeltaMultiFileReader::BindOptions(MultiFileReaderOptions &options, MultiFileList &files,
                 vector<LogicalType> &return_types, vector<string> &names, MultiFileReaderBindData& bind_data) {
    MultiFileReader::BindOptions(options, files, return_types, names, bind_data);

    //! TODO Hacky asf
    auto custom_bind_data = make_uniq<DeltaMultiFileReaderBindData>(dynamic_cast<DeltaTableSnapshot&>(files));

    auto demo_gen_col_opt = options.custom_options.find("delta_file_number");
    if (demo_gen_col_opt != options.custom_options.end()) {
        custom_bind_data->file_number_column_idx = names.size();
        names.push_back("delta_file_number");
        return_types.push_back(LogicalType::UBIGINT);
    }

    bind_data.custom_data = std::move(custom_bind_data);
}

void DeltaMultiFileReader::FinalizeBind(const MultiFileReaderOptions &file_options, const MultiFileReaderBindData &options,
                  const string &filename, const vector<string> &local_names,
                  const vector<LogicalType> &global_types, const vector<string> &global_names,
                  const vector<column_t> &global_column_ids, MultiFileReaderData &reader_data,
                  ClientContext &context) {
    MultiFileReader::FinalizeBind(file_options, options, filename, local_names, global_types, global_names, global_column_ids, reader_data, context);


    // The DeltaMultiFileReader specific finalization
    if (options.custom_data) {
        auto &custom_bind_data = dynamic_cast<DeltaMultiFileReaderBindData&>(*options.custom_data);
        if (custom_bind_data.file_number_column_idx != DConstants::INVALID_INDEX) {
            // TODO: remove the need for a placeholder here?
            reader_data.constant_map.emplace_back(custom_bind_data.file_number_column_idx, Value::UBIGINT(0));
        }

        // Add any constants from the Delta metadata to the reader partition map
        auto file_metadata = custom_bind_data.current_snapshot.metadata.find(filename);
        if (file_metadata != custom_bind_data.current_snapshot.metadata.end() && !file_metadata->second.partition_map.empty()) {
            for (idx_t i = 0; i < global_column_ids.size(); i++) {
                column_t col_id = global_column_ids[i];
                auto col_partition_entry = file_metadata->second.partition_map.find(global_names[col_id]);
                if (col_partition_entry != file_metadata->second.partition_map.end()) {
                    // Todo: use https://github.com/delta-io/delta/blob/master/PROTOCOL.md#partition-value-serialization
                    auto maybe_value = Value(col_partition_entry->second).DefaultCastAs(global_types[i]);
                    reader_data.constant_map.emplace_back(i, maybe_value);
                }
            }
        }
    }
}

unique_ptr<MultiFileList> DeltaMultiFileReader::GetFileList(ClientContext &context, const Value &input, const string &name,
                                                       FileGlobOptions options) {
    if (input.type() != LogicalType::VARCHAR) {
        throw BinderException("'delta_scan' only supports single path");
    }

    return make_uniq<DeltaTableSnapshot>(input.GetValue<string>());
}

static SelectionVector DuckSVFromDeltaSV(ffi::KernelBoolSlice *dv, idx_t offset, idx_t count, idx_t &select_count, idx_t &skip_count) {
    auto max_count = MinValue<idx_t>(count, dv->len - offset);
    SelectionVector result {max_count};

//    print_selection_vector(" cur: ", dv);

    idx_t current_select = 0;
    for (idx_t i = 0; i < max_count; i++) {
        if (dv->ptr[i + offset]) {
            result.data()[current_select] = i;
            current_select++;
        }
    }
    select_count = current_select;
    skip_count = max_count - select_count;

//    result.Print(select_count);

    return result;
}

void DeltaMultiFileReader::FinalizeChunk(ClientContext &context, const MultiFileReaderBindData &bind_data,
                   const MultiFileReaderData &reader_data, DataChunk &chunk, const string &filename) {
    // Base class finalization first
    MultiFileReader::FinalizeChunk(context, bind_data, reader_data, chunk, filename);

    if (bind_data.custom_data) {
        auto &custom_bind_data = dynamic_cast<DeltaMultiFileReaderBindData&>(*bind_data.custom_data);
        auto &metadata = custom_bind_data.current_snapshot.GetFileMetadata(filename);

        if (metadata.selection_vector.get() && chunk.size() != 0) {
            // Handle deletion vector
            idx_t select_count, skip_count;
            auto sv = DuckSVFromDeltaSV(metadata.selection_vector.get(), metadata.current_selection_vector_offset, STANDARD_VECTOR_SIZE, select_count, skip_count);
            metadata.current_selection_vector_offset += select_count + skip_count;
            chunk.Slice(sv, select_count);
        }

        // Note: this demo function shows how we can use DuckDB's Binder create expression-based generated columns
        if (custom_bind_data.file_number_column_idx != DConstants::INVALID_INDEX) {

            //! Create Dummy expression (0 + file_number)
            vector<unique_ptr<ParsedExpression>> child_expr;
            child_expr.push_back(make_uniq<ConstantExpression>(Value::UBIGINT(0)));
            child_expr.push_back(make_uniq<ConstantExpression>(Value::UBIGINT(metadata.file_number)));
            unique_ptr<ParsedExpression> expr = make_uniq<FunctionExpression>("+", std::move(child_expr), nullptr, nullptr, false, true);

            //! s dummy expression
            auto binder = Binder::CreateBinder(context);
            ExpressionBinder expr_binder(*binder, context);
            auto bound_expr = expr_binder.Bind(expr, nullptr);

            //! Execute dummy expression into result column
            ExpressionExecutor expr_executor(context);
            expr_executor.AddExpression(*bound_expr);

            //! Execute the expression directly into the output Chunk
            expr_executor.ExecuteExpression(chunk.data[custom_bind_data.file_number_column_idx]);
        }
    }
};

bool DeltaMultiFileReader::ParseOption(const string &key, const Value &val, MultiFileReaderOptions &options, ClientContext &context) {
    auto loption = StringUtil::Lower(key);

    if (loption == "delta_file_number") {
        options.custom_options[loption] = val;
        return true;
    }

    return MultiFileReader::ParseOption(key, val, options, context);
}

DeltaMultiFileReaderBindData::DeltaMultiFileReaderBindData(DeltaTableSnapshot & delta_table_snapshot): current_snapshot(delta_table_snapshot){

}

TableFunctionSet DeltatableFunctions::GetDeltaScanFunction(DatabaseInstance &instance) {
    // The delta_scan function is constructed by grabbing the parquet scan from the Catalog, then injecting the
    // DeltaMultiFileReader into it to create a Delta-based multi file read

    auto &parquet_scan = ExtensionUtil::GetTableFunction(instance, "parquet_scan");
    auto parquet_scan_copy = parquet_scan.functions;
    for (auto &function : parquet_scan_copy.functions) {
        // Register the MultiFileReader as the driver for reads
        function.get_multi_file_reader = DeltaMultiFileReader::CreateInstance;

        // Unset all of these: they are either broken, very inefficient.
        // TODO: implement/fix these
        function.serialize = nullptr;
        function.deserialize = nullptr;
        function.statistics = nullptr;
        function.table_scan_progress = nullptr;
        function.cardinality = nullptr;
        function.get_bind_info = nullptr;

        // Schema param is just confusing here
        function.named_parameters.erase("schema");

        // Demonstration of a generated column based on information from DeltaTableSnapshot
        function.named_parameters["delta_file_number"] = LogicalType::BOOLEAN;

        function.name = "delta_scan";
    }

    parquet_scan_copy.name = "delta_scan";
    return parquet_scan_copy;
}

} // namespace duckdb
