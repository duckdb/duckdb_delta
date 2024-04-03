#include "duckdb/function/table_function.hpp"

#include "delta_utils.hpp"
#include "deltatable_functions.hpp"
#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/common/multi_file_reader.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"

#include <string>
#include <numeric>

namespace duckdb {

//! The DeltaTableSnapshot implements the MultiFileList API to allow injecting it into the regular DuckDB parquet scan
struct DeltaTableSnapshot : public MultiFileList {
    string GetFile(idx_t i) override;
    //! (optional) Push down filters into the MultiFileList; sometimes the filters can be used to skip files completely
    bool ComplexFilterPushdown(ClientContext &context, const MultiFileReaderOptions &options, LogicalGet &get,
                                       vector<unique_ptr<Expression>> &filters) override;
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

struct DeltaMultiFilereader : public MultiFileReader {
    static unique_ptr<MultiFileReader> CreateInstance();

    //! Add the parameters for multi-file readers (e.g. union_by_name, filename) to a table function
    void AddParameters(TableFunction &table_function) override;
    //! Performs any globbing for the multi-file reader and returns a list of files to be read
    unique_ptr<MultiFileList> GetFileList(ClientContext &context, const Value &input, const string &name,
                                                             FileGlobOptions options = FileGlobOptions::DISALLOW_EMPTY) override;
    //! Tries to use the MultiFileReader for binding. This method can be overridden by custom MultiFileReaders
    bool Bind(MultiFileReaderOptions &options, MultiFileList &files,
                                 vector<LogicalType> &return_types, vector<string> &names, MultiFileReaderBindData &bind_data) override;
};

unique_ptr<MultiFileReader> DeltaMultiFilereader::CreateInstance() {
    return std::move(make_uniq<DeltaMultiFilereader>());
}

void DeltaMultiFilereader::AddParameters(TableFunction &table_function) {
    table_function.named_parameters["filename"] = LogicalType::BOOLEAN;
}

bool DeltaMultiFilereader::Bind(MultiFileReaderOptions &options, MultiFileList &files,
              vector<LogicalType> &return_types, vector<string> &names, MultiFileReaderBindData &bind_data)  {
    auto &delta_table_snapshot = dynamic_cast<DeltaTableSnapshot&>(files);

    auto schema = SchemaVisitor::VisitSnapshotSchema(delta_table_snapshot.snapshot);
    for (const auto &field: *schema) {
        names.push_back(field.first);
        return_types.push_back(field.second);
    }

    delta_table_snapshot.names = names;

    // Indicate we have performed a bind and no binding needs to occur on the parquet files themselves;
    return true;
};

unique_ptr<MultiFileList> DeltaMultiFilereader::GetFileList(ClientContext &context, const Value &input, const string &name,
                                                       FileGlobOptions options) {
    // Set path
    auto delta_file_list = make_uniq<DeltaTableSnapshot>();
    delta_file_list->path = input.GetValue<string>();

    // Set version
    auto path_slice = str_slice(delta_file_list->path);
    auto table_client_res = get_default_client(path_slice, error_allocator);
    const ffi::ExternTableClientHandle *table_client = unpack_result_or_throw(table_client_res, "get_default_client in DeltaScanScanBind");

    auto snapshot_res = snapshot(path_slice, table_client);
    const ffi::SnapshotHandle *snapshot = unpack_result_or_throw(snapshot_res, "snapshot in DeltaScanScanBind");
    delta_file_list->version = ffi::version(snapshot);

    delta_file_list->table_client = table_client;
    delta_file_list->snapshot = snapshot;

    return std::move(delta_file_list);
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

void DeltaTableSnapshot::InitializeFiles() {
    PredicateVisitor visitor(names, &table_filters);
    auto scan_files_result = ffi::kernel_scan_files_init(snapshot, table_client, &visitor);

    files = {
            unpack_result_or_throw(scan_files_result, "kernel_scan_files_init in InitGlobal"),
            ffi::kernel_scan_files_free,
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

TableFunctionSet DeltatableFunctions::GetDeltaScanFunction(DatabaseInstance &instance) {
    auto &parquet_scan = ExtensionUtil::GetTableFunction(instance, "parquet_scan");

    auto parquet_scan_copy = parquet_scan.functions;

    for (auto &function : parquet_scan_copy.functions) {
        //! Register the MultiFileReader as the driver for reads
        function.get_multi_file_reader = DeltaMultiFilereader::CreateInstance;
        function.serialize = nullptr;
        function.deserialize = nullptr;
        function.statistics = nullptr;
        function.table_scan_progress = nullptr;
        function.cardinality = nullptr;
        function.get_bind_info = nullptr;
        function.name = "delta_scan";
    }

    parquet_scan_copy.name = "delta_scan";
    return parquet_scan_copy;
}

} // namespace duckdb
