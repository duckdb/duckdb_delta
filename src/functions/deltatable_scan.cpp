#include "duckdb/function/table_function.hpp"

#include "delta_utils.hpp"
#include "parquet_reader.hpp"
#include "deltatable_functions.hpp"

#include <string>
#include <numeric>
#include <iostream>

namespace duckdb {

struct DeltaScanGlobalState : public GlobalTableFunctionState {
    bool done = false;

    UniqueKernelPointer <ffi::KernelScanFileIterator> files;
    vector<column_t> column_ids;
    optional_ptr<TableFilterSet> filters;
    std::atomic<int32_t> files_scanned = {0};
};

struct DeltaScanBindData : public TableFunctionData {
    string path;
    idx_t version;

    const ffi::SnapshotHandle *snapshot;
    const ffi::ExternTableClientHandle *table_client;

    vector<string> column_names;
    vector<LogicalType> column_types;
};

struct DeltaScanParquetReader {
    MultiFileReaderBindData reader_bind;
    ParquetReaderScanState scan_state;
    ParquetOptions parquet_options;
    ParquetReader reader;

    DeltaScanParquetReader(ClientContext &context, const DeltaScanBindData &bind_data, DeltaScanGlobalState &gstate,
                           string file)
            : parquet_options(context), reader(context, file, parquet_options) {
        MultiFileReader::InitializeReader(
                reader,
                {}, // options
                reader_bind,
                bind_data.column_types,
                bind_data.column_names,
                gstate.column_ids,
                gstate.filters,
                file,
                context
        );
        vector<idx_t> group_indexes(reader.metadata->metadata->row_groups.size());
        std::iota(group_indexes.begin(), group_indexes.end(), 0);
        reader.InitializeScan(scan_state, group_indexes);
    }

    void ReadOneChunk(DataChunk &output) {
        reader.Scan(scan_state, output);
        MultiFileReader::FinalizeChunk(reader_bind, reader.reader_data, output);
    }
};

struct DeltaScanLocalState : public LocalTableFunctionState {
    std::unique_ptr<DeltaScanParquetReader> reader;
};

// Try to claim one of the remaining files to read
static unique_ptr<DeltaScanParquetReader> TryBindNextFileScan(ClientContext &context,
                                                              const DeltaScanBindData &bind_data,
                                                              DeltaScanGlobalState &gstate) {
    struct Visitor {
        std::string file_name;
        bool file_name_valid = false;

        static void visit(void *data, ffi::KernelStringSlice name) {
            auto &v = *static_cast<Visitor *>(data);
            v.file_name = string(name.ptr, name.len);
            v.file_name_valid = true;
        }
    } visitor;
    kernel_scan_files_next(gstate.files.get(), &visitor, Visitor::visit);
    if (!visitor.file_name_valid) {
        return nullptr;
    }

    auto file_index = gstate.files_scanned++;
    std::string file = bind_data.path + "/" + visitor.file_name;
    return make_uniq<DeltaScanParquetReader>(context, bind_data, gstate, file);
}

static unique_ptr<LocalTableFunctionState> DeltaScanScanInitLocal(ExecutionContext &context,
                                                                  TableFunctionInitInput &input,
                                                                  GlobalTableFunctionState *gstate_p) {
    auto &bind_data = input.bind_data->Cast<DeltaScanBindData>();
    auto &gstate = gstate_p->Cast<DeltaScanGlobalState>();
    auto reader = TryBindNextFileScan(context.client, bind_data, gstate);
    if (!reader) {
        return nullptr; // we started too late, and other threads already claimed all files
    }
    auto result = make_uniq<DeltaScanLocalState>();
    result->reader = std::move(reader);
    return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> DeltaScanScanInitGlobal(ClientContext &context,
                                                                    TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->CastNoConst<DeltaScanBindData>();
    auto result = make_uniq<DeltaScanGlobalState>();

    PredicateVisitor visitor(bind_data.column_names, input.filters);
    auto scan_files_result = ffi::kernel_scan_files_init(bind_data.snapshot, bind_data.table_client, &visitor);

    result->files = {
            unpack_result_or_throw(scan_files_result, "kernel_scan_files_init in InitGlobal"),
            ffi::kernel_scan_files_free,
    };

    result->column_ids = input.column_ids;
    result->filters = input.filters;
    return std::move(result);
}


static unique_ptr<FunctionData> DeltaScanScanBind(ClientContext &context,
                                                  TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types,
                                                  vector<string> &names) {
    auto result = make_uniq<DeltaScanBindData>();
    result->path = input.inputs[0].GetValue<string>();

    auto path_slice = str_slice(result->path);

    auto table_client_res = get_default_client(path_slice, allocator_error_fun);
    const ffi::ExternTableClientHandle *table_client = unpack_result_or_throw(table_client_res, "get_default_client in DeltaScanScanBind");

    auto snapshot_res = snapshot(path_slice, table_client);
    const ffi::SnapshotHandle *snapshot = unpack_result_or_throw(snapshot_res, "snapshot in DeltaScanScanBind");

    result->version = ffi::version(snapshot);

    auto schema = SchemaVisitor::VisitSnapshotSchema(snapshot);
    for (const auto &field: *schema) {
        result->column_names.push_back(field.first);
        result->column_types.push_back(field.second);
    }

    // TODO: What the heck is this?? (copied blindly from parquet extension, seemed important)
    if (return_types.empty() && names.empty()) {
        return_types = result->column_types;
        names = result->column_names;
    } else {
        throw std::runtime_error(StringUtil::Format(
                "Unsupported: Analyzer requested %zd return_types and %zd names",
                return_types.size(), names.size()));
    }

    result->table_client = table_client;
    result->snapshot = snapshot;

    return std::move(result);
}

static void DeltaScanScanTableFun(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
    if (!input.local_state) {
        return; // this worker didn't even get a first file
    }

    auto &bind_data = input.bind_data->Cast<DeltaScanBindData>();
    auto &gstate = input.global_state->Cast<DeltaScanGlobalState>();
    auto &state = input.local_state->Cast<DeltaScanLocalState>();
    for (;;) {
        state.reader->ReadOneChunk(output);
        if (output.size() > 0) {
            return; // got some data!
        }

        auto reader = TryBindNextFileScan(context, bind_data, gstate);
        if (!reader) {
            return; // no more files to scan
        }

        // try again with the new file
        state.reader = std::move(reader);
    }
}

TableFunctionSet DeltatableFunctions::GetDeltaScanFunction() {
    TableFunctionSet function_set("delta_scan");

    auto fun = TableFunction("delta_scan", {LogicalType::VARCHAR}, DeltaScanScanTableFun, DeltaScanScanBind,
                             DeltaScanScanInitGlobal, DeltaScanScanInitLocal);
    function_set.AddFunction(fun);

    return function_set;
}

} // namespace duckdb
