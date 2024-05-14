#include "duckdb/function/table_function.hpp"

#include "deltatable_functions.hpp"
#include "functions/deltatable_scan.hpp"
#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/main/secret/secret_manager.hpp"


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

static void visit_callback(ffi::NullableCvoid engine_context, const struct ffi::KernelStringSlice path, int64_t size, const ffi::DvInfo *dv_info, struct ffi::CStringMap *partition_values) {
    auto context = (DeltaTableSnapshot *) engine_context;
    auto path_string =  context->GetPath() + "/" + from_delta_string_slice(path);

//    printf("Fetch metadata for %s\n", path_string.c_str());

    // First we append the file to our resolved files
    context->resolved_files.push_back(DeltaTableSnapshot::ToDuckDBPath(path_string));
    context->metadata.push_back({});

    D_ASSERT(context->resolved_files.size() == context->metadata.size());

    // Initialize the file metadata
    context->metadata.back().delta_snapshot_version = context->version;
    context->metadata.back().file_number = context->resolved_files.size() - 1;

    // Fetch the deletion vector
    auto selection_vector_res = ffi::selection_vector_from_dv(dv_info, context->table_client, context->global_state);
    auto selection_vector = unpack_result_or_throw(selection_vector_res, "selection_vector_from_dv for path " + context->GetPath());
    if (selection_vector) {
        context->metadata.back().selection_vector = {selection_vector, ffi::drop_bool_slice};
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
    context->metadata.back().partition_map = std::move(constant_map);
}

static void visit_data(void *engine_context, struct ffi::EngineDataHandle *engine_data, const struct ffi::KernelBoolSlice selection_vec) {
//    printf("Got some data\n");
//    printf("  Of this data, here is a selection vector\n");
//    print_selection_vector("    ", &selection_vec);
    ffi::visit_scan_data(engine_data, selection_vec, engine_context, visit_callback);
}

static ffi::EngineBuilder* CreateBuilder(ClientContext &context, const string &path) {
    ffi::EngineBuilder* builder;

    // For "regular" paths we early out with the default builder config
    if (!StringUtil::StartsWith(path, "s3://")) {
        auto interface_builder_res = ffi::get_engine_builder(to_delta_string_slice(path), error_allocator);
        return unpack_result_or_throw(interface_builder_res, "get_engine_interface_builder for path " + path);
    }

    auto end_of_container = path.find('/',5);

    if(end_of_container == string::npos) {
        throw IOException("Invalid s3 url passed to delta scan: %s", path);
    }
    auto bucket = path.substr(5, end_of_container-5);
    auto path_in_bucket = path.substr(end_of_container);

    auto interface_builder_res = ffi::get_engine_builder(to_delta_string_slice(path), error_allocator);
    builder = unpack_result_or_throw(interface_builder_res, "get_engine_interface_builder for path " + path);

//    ffi::set_builder_option(builder, to_delta_string_slice("aws_bucket"), to_delta_string_slice(bucket));

    // For S3 paths we need to trim the url, set the container, and fetch a potential secret
    auto &secret_manager = SecretManager::Get(context);
    auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);

    auto secret_match = secret_manager.LookupSecret(transaction, path, "s3");

    // No secret: nothing left to do here!
    if (!secret_match.HasMatch()) {
        return builder;
    }
    const auto &kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_match.secret_entry->secret);

    auto key_id = kv_secret.TryGetValue("key_id");
    auto secret = kv_secret.TryGetValue("secret");
    auto region = kv_secret.TryGetValue("region");
    auto endpoint = kv_secret.TryGetValue("endpoint");
    auto session_token = kv_secret.TryGetValue("session_token");

    ffi::set_builder_option(builder, to_delta_string_slice("aws_access_key_id"), to_delta_string_slice(key_id.ToString()));
    ffi::set_builder_option(builder, to_delta_string_slice("aws_secret_access_key"), to_delta_string_slice(secret.ToString()));
    ffi::set_builder_option(builder, to_delta_string_slice("aws_region"), to_delta_string_slice(region.ToString()));

    return builder;
}

DeltaTableSnapshot::DeltaTableSnapshot(ClientContext &context_p, const string &path) : MultiFileList({ToDeltaPath(path)}, FileGlobOptions::ALLOW_EMPTY), context(context_p) {
}

string DeltaTableSnapshot::GetPath() {
    return GetPaths()[0];
}

string DeltaTableSnapshot::ToDuckDBPath(const string &raw_path) {
    if (StringUtil::StartsWith(raw_path, "file://")) {
        return raw_path.substr(7);
    }
    return raw_path;
}

string DeltaTableSnapshot::ToDeltaPath(const string &raw_path) {
    string path;
    if (StringUtil::StartsWith(raw_path, "./")) {
        LocalFileSystem fs;
        path = fs.JoinPath(fs.GetWorkingDirectory(), raw_path.substr(2));
        path = "file://" + path;
    } else {
        path = raw_path;
    }
    return path;
}

void DeltaTableSnapshot::Bind(vector<LogicalType> &return_types, vector<string> &names) {
    if (!initialized) {
        InitializeFiles();
    }
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

    if (files_exhausted) {
        return "";
    }

    while(i >= resolved_files.size()) {
        auto size_before = resolved_files.size();

        auto have_scan_data_res = ffi::kernel_scan_data_next(scan_data_iterator.get(), this, visit_data);

        auto have_scan_data = unpack_result_or_throw(have_scan_data_res, "kernel_scan_data_next in DeltaTableSnapshot GetFile");

        // TODO: shouldn't the kernel always return false here?
        if (!have_scan_data || resolved_files.size() == size_before) {
            files_exhausted = true;
            return "";
        }
    }

    // The kernel scan visitor should have resolved a file OR returned
    if(i >= resolved_files.size()) {
        throw InternalException("Delta Kernel seems to have failed to resolve a new file");
    }

    return resolved_files[i];
}

void DeltaTableSnapshot::InitializeFiles() {
    auto path_slice = to_delta_string_slice(paths[0]);

    auto interface_builder = CreateBuilder(context, paths[0]);
    auto engine_interface_res = ffi::builder_build(interface_builder);
    table_client = unpack_result_or_throw(engine_interface_res, "get_default_client in DeltaScanScanBind");

    // Alternatively we can do the default client like so:
//    auto table_client_res = ffi::get_default_client(path_slice, error_allocator);
//    table_client = unpack_result_or_throw(table_client_res, "get_default_client in DeltaScanScanBind");

    // Initialize Snapshot
    auto snapshot_res = ffi::snapshot(path_slice, table_client);
    snapshot = unpack_result_or_throw(snapshot_res, "snapshot in DeltaScanScanBind");

    PredicateVisitor visitor(names, &table_filters);

    auto scan_res = ffi::scan(snapshot, table_client, &visitor);
    scan = unpack_result_or_throw(scan_res, "scan in DeltaScanScanBind");

    global_state = ffi::get_global_scan_state(scan);

    // Set version
    this->version = ffi::version(snapshot);

    auto scan_iterator_res = ffi::kernel_scan_data_init(table_client, scan);
    scan_data_iterator = {
            unpack_result_or_throw(scan_iterator_res, "kernel_scan_data_init in InitFiles"),
            ffi::kernel_scan_data_free
    };

    initialized = true;
}

unique_ptr<MultiFileList> DeltaTableSnapshot::ComplexFilterPushdown(ClientContext &context, const MultiFileReaderOptions &options, LogicalGet &get,
                                               vector<unique_ptr<Expression>> &filters) {
    FilterCombiner combiner(context);
    for (const auto &filter : filters) {
        combiner.AddFilter(filter->Copy());
    }
    auto filterstmp = combiner.GenerateTableScanFilters(get.column_ids);

    // TODO: can/should we figure out if this filtered anything?
    // TODO2: make this copy more efficient? can we move-copy this thing leaving the old one uninitialized?
    auto filtered_list = make_uniq<DeltaTableSnapshot>(context, paths[0]);
    filtered_list->table_filters = std::move(filterstmp);
    filtered_list->names = names;

    return filtered_list;
}

vector<string> DeltaTableSnapshot::GetAllFiles() {
    idx_t i = resolved_files.size();
    // TODO: this can probably be improved
    while(!GetFile(i).empty()) {
        i++;
    }
    return resolved_files;
}

FileExpandResult DeltaTableSnapshot::GetExpandResult() {
    // GetFile(1) will ensure at least the first 2 files are expanded if they are available
    GetFile(1);

    if (resolved_files.size() > 1) {
        return FileExpandResult::MULTIPLE_FILES;
    } else if (resolved_files.size() == 1) {
        return FileExpandResult::SINGLE_FILE;
    }

    return FileExpandResult::NO_FILES;
}

idx_t DeltaTableSnapshot::GetTotalFileCount() {
    // TODO: this can probably be improved
    idx_t i = resolved_files.size();
    while(!GetFile(i).empty()) {
        i++;
    }
    return resolved_files.size();
}

unique_ptr<MultiFileReader> DeltaMultiFileReader::CreateInstance() {
    return std::move(make_uniq<DeltaMultiFileReader>());
}

bool DeltaMultiFileReader::Bind(MultiFileReaderOptions &options, MultiFileList &files,
              vector<LogicalType> &return_types, vector<string> &names, MultiFileReaderBindData &bind_data)  {
    auto &delta_table_snapshot = dynamic_cast<DeltaTableSnapshot&>(files);

    delta_table_snapshot.Bind(return_types, names);

    // We need to parse this option
    bool file_row_number_enabled = options.custom_options.find("file_row_number") != options.custom_options.end();
    if (file_row_number_enabled) {
        bind_data.file_row_number_idx = names.size();
        return_types.emplace_back(LogicalType::BIGINT);
        names.emplace_back("file_row_number");
    } else {
        // TODO: this is a bogus ID? Change for flag indicating it should be enabled?
        bind_data.file_row_number_idx = names.size();
    }

    return true;
};

void DeltaMultiFileReader::BindOptions(MultiFileReaderOptions &options, MultiFileList &files,
                 vector<LogicalType> &return_types, vector<string> &names, MultiFileReaderBindData& bind_data) {

    // Disable all other multifilereader options
    options.auto_detect_hive_partitioning = false;
    options.hive_partitioning = false;
    options.union_by_name = false;

    MultiFileReader::BindOptions(options, files, return_types, names, bind_data);

    auto demo_gen_col_opt = options.custom_options.find("delta_file_number");
    if (demo_gen_col_opt != options.custom_options.end()) {
        if (demo_gen_col_opt->second.GetValue<bool>()) {
            names.push_back("delta_file_number");
            return_types.push_back(LogicalType::UBIGINT);
        }
    }
}

void DeltaMultiFileReader::FinalizeBind(const MultiFileReaderOptions &file_options, const MultiFileReaderBindData &options,
                  const string &filename, const vector<string> &local_names,
                  const vector<LogicalType> &global_types, const vector<string> &global_names,
                  const vector<column_t> &global_column_ids, MultiFileReaderData &reader_data,
                  ClientContext &context, optional_ptr<MultiFileReaderGlobalState> global_state) {
    MultiFileReader::FinalizeBind(file_options, options, filename, local_names, global_types, global_names, global_column_ids, reader_data, context, global_state);

    // Handle custom delta option set in MultiFileReaderOptions::custom_options
    auto file_number_opt = file_options.custom_options.find("delta_file_number");
    if (file_number_opt != file_options.custom_options.end()) {
        if (file_number_opt->second.GetValue<bool>()) {
            D_ASSERT(global_state);
            auto &delta_global_state = global_state->Cast<DeltaMultiFileReaderGlobalState>();
            D_ASSERT(delta_global_state.delta_file_number_idx != DConstants::INVALID_INDEX);

            // We add the constant column for the delta_file_number option
            // NOTE: we add a placeholder here, to demonstrate how we can also populate extra columns in the FinalizeChunk
            reader_data.constant_map.emplace_back(delta_global_state.delta_file_number_idx, Value::UBIGINT(0));
        }
    }

    // Get the metadata for this file
    D_ASSERT(global_state->file_list);
    const auto &snapshot = dynamic_cast<const DeltaTableSnapshot&>(*global_state->file_list);
    auto &file_metadata = snapshot.metadata[reader_data.file_list_idx.GetIndex()];

    if (!file_metadata.partition_map.empty()) {
        for (idx_t i = 0; i < global_column_ids.size(); i++) {
            column_t col_id = global_column_ids[i];
            auto col_partition_entry = file_metadata.partition_map.find(global_names[col_id]);
            if (col_partition_entry != file_metadata.partition_map.end()) {
                // Todo: use https://github.com/delta-io/delta/blob/master/PROTOCOL.md#partition-value-serialization
                auto maybe_value = Value(col_partition_entry->second).DefaultCastAs(global_types[i]);
                reader_data.constant_map.emplace_back(i, maybe_value);
            }
        }
    }
}

unique_ptr<MultiFileList> DeltaMultiFileReader::CreateFileList(ClientContext &context, const vector<string>& paths, FileGlobOptions options) {
    if (paths.size() != 1) {
        throw BinderException("'delta_scan' only supports single path as input");
    }

    return make_uniq<DeltaTableSnapshot>(context, paths[0]);
}

// Generate the correct Selection Vector Based on the Raw delta KernelBoolSlice dv and the row_id_column
// TODO: this probably is slower than needed (we can do with less branches in the for loop for most cases)
static SelectionVector DuckSVFromDeltaSV(ffi::KernelBoolSlice *dv, Vector row_id_column, idx_t count, idx_t &select_count) {
    D_ASSERT(row_id_column.GetType() == LogicalType::BIGINT);

    UnifiedVectorFormat data;
    row_id_column.ToUnifiedFormat(count, data);
    auto row_ids = UnifiedVectorFormat::GetData<int64_t>(data);

    SelectionVector result {count};
    idx_t current_select = 0;
    for (idx_t i = 0; i < count; i++) {
        auto row_id = row_ids[data.sel->get_index(i)];

        // TODO: why are deletion vectors not spanning whole data?
        if (row_id >= dv->len || dv->ptr[row_id]) {
            result.data()[current_select] = i;
            current_select++;
        }
    }

    select_count = current_select;

    return result;
}

// Parses the columns that are used by the delta extension into
void DeltaMultiFileReaderGlobalState::SetColumnIdx(const string &column, idx_t idx) {
    if (column == "file_row_number") {
        file_row_number_idx = idx;
        return;
    } else if (column == "delta_file_number") {
        delta_file_number_idx = idx;
        return;
    }
    throw InternalException("Unknown column '%s' found as required by the DeltaMultiFileReader");
}

unique_ptr<MultiFileReaderGlobalState> DeltaMultiFileReader::InitializeGlobalState(duckdb::ClientContext &context,
                                                                                   const duckdb::MultiFileReaderOptions &file_options,
                                                                                   const duckdb::MultiFileReaderBindData &bind_data,
                                                                                   const duckdb::MultiFileList &file_list,
                                                                                   const vector<duckdb::LogicalType> &global_types,
                                                                                   const vector<std::string> &global_names,
                                                                                   const vector<duckdb::column_t> &global_column_ids) {
    vector<LogicalType> extra_columns;
    vector<pair<string, idx_t>> mapped_columns;

    // Create a map of the columns that are in the projection
    case_insensitive_map_t<idx_t> selected_columns;
    for (idx_t i = 0; i < global_column_ids.size(); i++) {
        auto global_id = global_column_ids[i];
        auto &global_name = global_names[global_id];
        selected_columns.insert({global_name, i});
    }

    // The hardcoded (for now) columns to be mapped
    case_insensitive_map_t<LogicalType> columns_to_map = {
            {"file_row_number", LogicalType::BIGINT},
            {"delta_file_number", LogicalType::UBIGINT}
    };

    // Map every column to either a column in the projection, or add it to the extra columns if it doesn't exist
    idx_t col_offset = 0;
    for (const auto &required_column : columns_to_map) {
        // First check if the column is in the projection
        auto res = selected_columns.find(required_column.first);
        if (res != selected_columns.end()) {
            // The column is in the projection, no special handling is required; we simply store the index
            mapped_columns.push_back({required_column.first, res->second});
            continue;
        }

        // The column is NOT in the projection: it needs to be added as an extra_column

        // Calculate the index of the added column (extra columns are added after all other columns)
        idx_t current_col_idx = global_column_ids.size() + col_offset++;

        // Add column to the map, to ensure the MultiFileReader can find it when processing the Chunk
        mapped_columns.push_back({required_column.first, current_col_idx});

        // Ensure the result DataChunk has a vector of the correct type to store this column
        extra_columns.push_back(required_column.second);
    }

    auto res = make_uniq<DeltaMultiFileReaderGlobalState>(extra_columns, &file_list);

    // Parse all the mapped columns into the DeltaMultiFileReaderGlobalState for easy use;
    for (const auto& mapped_column : mapped_columns) {
        res->SetColumnIdx(mapped_column.first, mapped_column.second);
    }

    return std::move(res);
}

void DeltaMultiFileReader::CreateNameMapping(const string &file_name, const vector<LogicalType> &local_types,
                                        const vector<string> &local_names, const vector<LogicalType> &global_types,
                                        const vector<string> &global_names, const vector<column_t> &global_column_ids,
                                        MultiFileReaderData &reader_data, const string &initial_file,
                                        optional_ptr<MultiFileReaderGlobalState> global_state) {
    // First call the base implementation to do most mapping
    MultiFileReader::CreateNameMapping(file_name, local_types, local_names, global_types, global_names, global_column_ids, reader_data, initial_file, global_state);

    // Then we handle delta specific mapping
    D_ASSERT(global_state);
    auto &delta_global_state = global_state->Cast<DeltaMultiFileReaderGlobalState>();

    // Check if the file_row_number column is an "extra_column" which is not part of the projection
    if (delta_global_state.file_row_number_idx >= global_column_ids.size()) {
        D_ASSERT(delta_global_state.file_row_number_idx != DConstants::INVALID_INDEX);

        // Build the name map
        case_insensitive_map_t<idx_t> name_map;
        for (idx_t col_idx = 0; col_idx < local_names.size(); col_idx++) {
            name_map[local_names[col_idx]] = col_idx;
        }

        // Lookup the required column in the local map
        auto entry = name_map.find("file_row_number");
        if (entry == name_map.end()) {
            throw InternalException("Failed to find the file_row_number column");
        }

        // Register the column to be scanned from this file
        reader_data.column_ids.push_back(entry->second);
        reader_data.column_mapping.push_back(delta_global_state.file_row_number_idx);
    }

    // This may have changed: update it
    reader_data.empty_columns = reader_data.column_ids.empty();
}

void DeltaMultiFileReader::FinalizeChunk(ClientContext &context, const MultiFileReaderBindData &bind_data,
                   const MultiFileReaderData &reader_data, DataChunk &chunk, optional_ptr<MultiFileReaderGlobalState> global_state) {
    // Base class finalization first
    MultiFileReader::FinalizeChunk(context, bind_data, reader_data, chunk, global_state);

    D_ASSERT(global_state);
    auto &delta_global_state = global_state->Cast<DeltaMultiFileReaderGlobalState>();
    D_ASSERT(delta_global_state.file_list);

    // Get the metadata for this file
    const auto &snapshot = dynamic_cast<const DeltaTableSnapshot&>(*global_state->file_list);
    auto &metadata = snapshot.metadata[reader_data.file_list_idx.GetIndex()];

    if (metadata.selection_vector.get() && chunk.size() != 0) {
        D_ASSERT(delta_global_state.file_row_number_idx != DConstants::INVALID_INDEX);
        auto &file_row_number_column = chunk.data[delta_global_state.file_row_number_idx];

        // Construct the selection vector using the file_row_number column and the raw selection vector from delta
        idx_t select_count;
        auto sv = DuckSVFromDeltaSV(metadata.selection_vector.get(), file_row_number_column, chunk.size(), select_count);
        chunk.Slice(sv, select_count);
    }

    // Note: this demo function shows how we can use DuckDB's Binder create expression-based generated columns
    if (delta_global_state.delta_file_number_idx != DConstants::INVALID_INDEX) {
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
        expr_executor.ExecuteExpression(chunk.data[delta_global_state.delta_file_number_idx]);
    }
};

bool DeltaMultiFileReader::ParseOption(const string &key, const Value &val, MultiFileReaderOptions &options, ClientContext &context) {
    auto loption = StringUtil::Lower(key);

    if (loption == "delta_file_number") {
        options.custom_options[loption] = val;
        return true;
    }

    // We need to capture this one to know whether to emit
    if (loption == "file_row_number") {
        options.custom_options[loption] = val;
        return true;
    }

    return MultiFileReader::ParseOption(key, val, options, context);
}
//
//DeltaMultiFileReaderBindData::DeltaMultiFileReaderBindData(DeltaTableSnapshot & delta_table_snapshot): current_snapshot(delta_table_snapshot){
//
//}

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
