#include "duckdb/function/table_function.hpp"

#include "delta_utils.hpp"
#include "parquet_reader.hpp"
#include "parquet_scan.hpp"
#include "deltatable_functions.hpp"
#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

#include <string>
#include <numeric>

namespace duckdb {

class DeltaTableMetadataProvider : public ParquetMetadataProvider {
public:
    bool HaveData() override;

    shared_ptr<ParquetReader> GetInitialReader() override;
    vector<shared_ptr<ParquetReader>> GetInitializedReaders() override;

    //! Get files and
    string GetFile(idx_t i) override;
    // TODO: figure out what this signature should look like
    string GetDeletionVector(string) override;

    void FilterPushdown(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
                        vector<unique_ptr<Expression>> &filters) override;
    unique_ptr<BaseStatistics> ParquetScanStats(ClientContext &context, const FunctionData *bind_data_p,
                                                column_t column_index) override;
    double ParquetProgress(ClientContext &context, const FunctionData *bind_data_p,
                           const GlobalTableFunctionState *global_state) override;
    unique_ptr<NodeStatistics> ParquetCardinality(ClientContext &context, const FunctionData *bind_data) override;
    idx_t ParquetScanMaxThreads(ClientContext &context, const FunctionData *bind_data) override;

public:
    // Overview
    string path;
    idx_t version;

    // Delta's file generation
    const ffi::SnapshotHandle *snapshot;
    const ffi::ExternTableClientHandle *table_client;
    UniqueKernelPointer <ffi::KernelScanFileIterator> files;

    // TODO: this is now copied all over the place, clean it up.
    vector<string> names;

    bool initialized = false;
    vector<string> resolved_files;
    void InitializeFiles();

    TableFilterSet table_filters;
};

bool DeltaTableMetadataProvider::HaveData() {
    if (!initialized && !table_filters.filters.empty()) {
        InitializeFiles();
    }

    // This ensures the first file is fetched if it exists
    GetFile(0);

    if (resolved_files.empty()) {
        return false;
    }

    return true;
}

shared_ptr<ParquetReader> DeltaTableMetadataProvider::GetInitialReader() {
    // Note: we don't need to open any parquet files so we return nothing here
    return nullptr;
}

vector<shared_ptr<ParquetReader>> DeltaTableMetadataProvider::GetInitializedReaders() {
    // Note: we don't need to open any parquet files so we return nothing here
    return {};
}

string DeltaTableMetadataProvider::GetFile(idx_t i) {
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

    struct Visitor {
        std::string file_name;
        bool file_name_valid = false;

        static void visit(void *data, ffi::KernelStringSlice name) {
            auto &v = *static_cast<Visitor *>(data);
            v.file_name = string(name.ptr, name.len);
            v.file_name_valid = true;
        }
    } visitor;
    kernel_scan_files_next(files.get(), &visitor, Visitor::visit);
    if (!visitor.file_name_valid) {
        return "";
    }

    auto new_file = path + "/" + visitor.file_name;
    resolved_files.push_back(new_file);
    return new_file;
}

string DeltaTableMetadataProvider::GetDeletionVector(string) {
    throw NotImplementedException("Not implemented");
}

void DeltaTableMetadataProvider::InitializeFiles() {
    // TODO pass filters here!
    PredicateVisitor visitor(names, &table_filters);
    auto scan_files_result = ffi::kernel_scan_files_init(snapshot, table_client, &visitor);

    files = {
            unpack_result_or_throw(scan_files_result, "kernel_scan_files_init in InitGlobal"),
            ffi::kernel_scan_files_free,
    };
    initialized = true;
}

void DeltaTableMetadataProvider::FilterPushdown(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
                                               vector<unique_ptr<Expression>> &filters) {
    FilterCombiner combiner(context);
    for (const auto &filter : filters) {
        combiner.AddFilter(filter->Copy());
    }
    table_filters = combiner.GenerateTableScanFilters(get.column_ids);
}

unique_ptr<BaseStatistics> DeltaTableMetadataProvider::ParquetScanStats(ClientContext &context, const FunctionData *bind_data_p,
                                                                       column_t column_index) {
    // NOTE: not implemented
    return nullptr;
}

double DeltaTableMetadataProvider::ParquetProgress(ClientContext &context, const FunctionData *bind_data_p,
                                                  const GlobalTableFunctionState *global_state) {
    // TODO: do clever things for parquet progress
    return 0.0;
}

unique_ptr<NodeStatistics> DeltaTableMetadataProvider::ParquetCardinality(ClientContext &context, const FunctionData *bind_data) {
    // TODO: do clever things for cardinality
    return nullptr;
}

idx_t DeltaTableMetadataProvider::ParquetScanMaxThreads(ClientContext &context, const FunctionData *bind_data) {
    // TODO: read the metadata here
    return TaskScheduler::GetScheduler(context).NumberOfThreads();
}

static unique_ptr<FunctionData> DeltaScanScanBind(ClientContext &context,
                                                  TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types,
                                                  vector<string> &names) {
    auto result = make_uniq<ParquetReadBindData>();

    // Set path
    auto metadata_provider = make_uniq<DeltaTableMetadataProvider>();
    metadata_provider->path = input.inputs[0].GetValue<string>();

    // Set version
    auto path_slice = str_slice(metadata_provider->path);
    auto table_client_res = get_default_client(path_slice, error_allocator);
    const ffi::ExternTableClientHandle *table_client = unpack_result_or_throw(table_client_res, "get_default_client in DeltaScanScanBind");

    auto snapshot_res = snapshot(path_slice, table_client);
    const ffi::SnapshotHandle *snapshot = unpack_result_or_throw(snapshot_res, "snapshot in DeltaScanScanBind");
    metadata_provider->version = ffi::version(snapshot);

    // Parse schema
    auto schema = SchemaVisitor::VisitSnapshotSchema(snapshot);
    for (const auto &field: *schema) {
        result->names.push_back(field.first);
        result->types.push_back(field.second);
    }
    return_types = result->types;
    names = result->names;

    metadata_provider->names = names;

    // Pass through Delta Kernel structures
    metadata_provider->table_client = table_client;
    metadata_provider->snapshot = snapshot;

    result->metadata_provider = std::move(metadata_provider);
    return std::move(result);
}

TableFunctionSet DeltatableFunctions::GetDeltaScanFunction() {
    TableFunctionSet function_set("delta_scan");

    auto fun = ParquetScanFunction::CreateParquetScan("delta_scan", DeltaScanScanBind, nullptr, nullptr);
    fun.get_bind_info = nullptr;
    function_set.AddFunction(fun);

    return function_set;
}

} // namespace duckdb
