#define DUCKDB_EXTENSION_MAIN

#include "parquet_override.hpp"
#include "parquet_extension.hpp"

#include "cast_column_reader.hpp"
#include "duckdb.hpp"
#include "parquet_crypto.hpp"
#include "parquet_metadata.hpp"
#include "parquet_reader.hpp"
#include "parquet_writer.hpp"
#include "struct_column_reader.hpp"
#include "zstd_file_system.hpp"

#include <fstream>
#include <iostream>
#include <numeric>
#include <string>
#include <vector>
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/helper.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/enums/file_compression_type.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/multi_file_reader.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table/row_group.hpp"
#endif

namespace duckdb {

struct ParquetReadBindData : public TableFunctionData {
	shared_ptr<MultiFileList> file_list;
	unique_ptr<MultiFileReader> multi_file_reader;

	shared_ptr<ParquetReader> initial_reader;
	atomic<idx_t> chunk_count;
	vector<string> names;
	vector<LogicalType> types;

	// The union readers are created (when parquet union_by_name option is on) during binding
	// Those readers can be re-used during ParquetParallelStateNext
	vector<shared_ptr<ParquetReader>> union_readers;

	// These come from the initial_reader, but need to be stored in case the initial_reader is removed by a filter
	idx_t initial_file_cardinality;
	idx_t initial_file_row_groups;
	ParquetOptions parquet_options;

	MultiFileReaderBindData reader_bind;

	void Initialize(shared_ptr<ParquetReader> reader) {
		initial_reader = std::move(reader);
		initial_file_cardinality = initial_reader->NumRows();
		initial_file_row_groups = initial_reader->NumRowGroups();
		parquet_options = initial_reader->parquet_options;
	}
};

struct ParquetReadLocalState : public LocalTableFunctionState {
	shared_ptr<ParquetReader> reader;
	ParquetReaderScanState scan_state;
	bool is_parallel;
	idx_t batch_index;
	idx_t file_index;
	//! The DataChunk containing all read columns (even columns that are immediately removed)
	DataChunk all_columns;
};

enum class ParquetFileState : uint8_t { UNOPENED, OPENING, OPEN, CLOSED };

struct ParquetFileReaderData {
	// Create data for an unopened file
	explicit ParquetFileReaderData(const string &file_to_be_opened)
	    : reader(nullptr), file_state(ParquetFileState::UNOPENED), file_mutex(make_uniq<mutex>()),
	      file_to_be_opened(file_to_be_opened) {
	}
	// Create data for an existing reader
	explicit ParquetFileReaderData(shared_ptr<ParquetReader> reader_p)
	    : reader(std::move(reader_p)), file_state(ParquetFileState::OPEN), file_mutex(make_uniq<mutex>()) {
	}

	//! Currently opened reader for the file
	shared_ptr<ParquetReader> reader;
	//! Flag to indicate the file is being opened
	ParquetFileState file_state;
	//! Mutexes to wait for the file when it is being opened
	unique_ptr<mutex> file_mutex;

	//! (only set when file_state is UNOPENED) the file to be opened
	string file_to_be_opened;
};

struct ParquetReadGlobalState : public GlobalTableFunctionState {
	//! The scan over the file_list
	MultiFileListScanData file_list_scan;

	unique_ptr<MultiFileReaderGlobalState> multi_file_reader_state;

	mutex lock;

	//! The current set of parquet readers
	vector<ParquetFileReaderData> readers;

	//! Signal to other threads that a file failed to open, letting every thread abort.
	bool error_opening_file = false;

	//! Index of file currently up for scanning
	atomic<idx_t> file_index;
	//! Index of row group within file currently up for scanning
	idx_t row_group_index;
	//! Batch index of the next row group to be scanned
	idx_t batch_index;

	idx_t max_threads;
	vector<idx_t> projection_ids;
	vector<LogicalType> scanned_types;
	vector<column_t> column_ids;
	TableFilterSet *filters;

	idx_t MaxThreads() const override {
		return max_threads;
	}

	bool CanRemoveColumns() const {
		return !projection_ids.empty();
	}
};

struct ParquetWriteBindData : public TableFunctionData {
	vector<LogicalType> sql_types;
	vector<string> column_names;
	duckdb_parquet::format::CompressionCodec::type codec = duckdb_parquet::format::CompressionCodec::SNAPPY;
	vector<pair<string, string>> kv_metadata;
	idx_t row_group_size = Storage::ROW_GROUP_SIZE;

	//! If row_group_size_bytes is not set, we default to row_group_size * BYTES_PER_ROW
	static constexpr const idx_t BYTES_PER_ROW = 1024;
	idx_t row_group_size_bytes;

	//! How/Whether to encrypt the data
	shared_ptr<ParquetEncryptionConfig> encryption_config;

	//! Dictionary compression is applied only if the compression ratio exceeds this threshold
	double dictionary_compression_ratio_threshold = 1.0;

	ChildFieldIDs field_ids;
	//! The compression level, higher value is more
	optional_idx compression_level;
};

struct ParquetWriteGlobalState : public GlobalFunctionData {
	unique_ptr<ParquetWriter> writer;
};

struct ParquetWriteLocalState : public LocalFunctionData {
	explicit ParquetWriteLocalState(ClientContext &context, const vector<LogicalType> &types)
	    : buffer(context, types, ColumnDataAllocatorType::HYBRID) {
		buffer.InitializeAppend(append_state);
	}

	ColumnDataCollection buffer;
	ColumnDataAppendState append_state;
};

static BindInfo ParquetGetBindInfo(const optional_ptr<FunctionData> bind_data) {
	auto bind_info = BindInfo(ScanType::PARQUET);
	auto &parquet_bind = bind_data->Cast<ParquetReadBindData>();

	vector<Value> file_path;
	for (const auto &file : parquet_bind.file_list->Files()) {
		file_path.emplace_back(file);
	}

	// LCOV_EXCL_START
	bind_info.InsertOption("file_path", Value::LIST(LogicalType::VARCHAR, file_path));
	bind_info.InsertOption("binary_as_string", Value::BOOLEAN(parquet_bind.parquet_options.binary_as_string));
	bind_info.InsertOption("file_row_number", Value::BOOLEAN(parquet_bind.parquet_options.file_row_number));
	parquet_bind.parquet_options.file_options.AddBatchInfo(bind_info);
	// LCOV_EXCL_STOP
	return bind_info;
}

static void ParseFileRowNumberOption(MultiFileReaderBindData &bind_data, ParquetOptions &options,
                                     vector<LogicalType> &return_types, vector<string> &names) {
	if (options.file_row_number) {
		if (StringUtil::CIFind(names, "file_row_number") != DConstants::INVALID_INDEX) {
			throw BinderException(
			    "Using file_row_number option on file with column named file_row_number is not supported");
		}

		bind_data.file_row_number_idx = names.size();
		return_types.emplace_back(LogicalType::BIGINT);
		names.emplace_back("file_row_number");
	}
}

static MultiFileReaderBindData BindSchema(ClientContext &context, vector<LogicalType> &return_types,
                                          vector<string> &names, ParquetReadBindData &result, ParquetOptions &options) {
	D_ASSERT(!options.schema.empty());

	options.file_options.AutoDetectHivePartitioning(*result.file_list, context);

	auto &file_options = options.file_options;
	if (file_options.union_by_name || file_options.hive_partitioning) {
		throw BinderException("Parquet schema cannot be combined with union_by_name=true or hive_partitioning=true");
	}

	vector<string> schema_col_names;
	vector<LogicalType> schema_col_types;
	schema_col_names.reserve(options.schema.size());
	schema_col_types.reserve(options.schema.size());
	for (const auto &column : options.schema) {
		schema_col_names.push_back(column.name);
		schema_col_types.push_back(column.type);
	}

	// perform the binding on the obtained set of names + types
	MultiFileReaderBindData bind_data;
	result.multi_file_reader->BindOptions(options.file_options, *result.file_list, schema_col_types, schema_col_names,
	                                      bind_data);

	names = schema_col_names;
	return_types = schema_col_types;
	D_ASSERT(names.size() == return_types.size());

	ParseFileRowNumberOption(bind_data, options, return_types, names);

	return bind_data;
}

static void InitializeParquetReader(ParquetReader &reader, const ParquetReadBindData &bind_data,
                                    const vector<column_t> &global_column_ids,
                                    optional_ptr<TableFilterSet> table_filters, ClientContext &context,
                                    optional_idx file_idx, optional_ptr<MultiFileReaderGlobalState> reader_state) {
	auto &parquet_options = bind_data.parquet_options;
	auto &reader_data = reader.reader_data;

	// Mark the file in the file list we are scanning here
	reader_data.file_list_idx = file_idx;

	if (bind_data.parquet_options.schema.empty()) {
		bind_data.multi_file_reader->InitializeReader(
		    reader, parquet_options.file_options, bind_data.reader_bind, bind_data.types, bind_data.names,
		    global_column_ids, table_filters, bind_data.file_list->GetFirstFile(), context, reader_state);
		return;
	}

	// a fixed schema was supplied, initialize the MultiFileReader settings here so we can read using the schema

	// this deals with hive partitioning and filename=true
	bind_data.multi_file_reader->FinalizeBind(parquet_options.file_options, bind_data.reader_bind, reader.GetFileName(),
	                                          reader.GetNames(), bind_data.types, bind_data.names, global_column_ids,
	                                          reader_data, context, reader_state);

	// create a mapping from field id to column index in file
	unordered_map<uint32_t, idx_t> field_id_to_column_index;
	auto &column_readers = reader.root_reader->Cast<StructColumnReader>().child_readers;
	for (idx_t column_index = 0; column_index < column_readers.size(); column_index++) {
		auto &column_schema = column_readers[column_index]->Schema();
		if (column_schema.__isset.field_id) {
			field_id_to_column_index[column_schema.field_id] = column_index;
		}
	}

	// loop through the schema definition
	for (idx_t i = 0; i < global_column_ids.size(); i++) {
		auto global_column_index = global_column_ids[i];

		// check if this is a constant column
		bool constant = false;
		for (auto &entry : reader_data.constant_map) {
			if (entry.column_id == i) {
				constant = true;
				break;
			}
		}
		if (constant) {
			// this column is constant for this file
			continue;
		}

		// Handle any generate columns that are not in the schema (currently only file_row_number)
		if (global_column_index >= parquet_options.schema.size()) {
			if (bind_data.reader_bind.file_row_number_idx == global_column_index) {
				reader_data.column_mapping.push_back(i);
				reader_data.column_ids.push_back(reader.file_row_number_idx);
			}
			continue;
		}

		const auto &column_definition = parquet_options.schema[global_column_index];
		auto it = field_id_to_column_index.find(column_definition.field_id);
		if (it == field_id_to_column_index.end()) {
			// field id not present in file, use default value
			reader_data.constant_map.emplace_back(i, column_definition.default_value);
			continue;
		}

		const auto &local_column_index = it->second;
		auto &column_reader = column_readers[local_column_index];
		if (column_reader->Type() != column_definition.type) {
			// differing types, wrap in a cast column reader
			reader_data.cast_map[local_column_index] = column_definition.type;
		}

		reader_data.column_mapping.push_back(i);
		reader_data.column_ids.push_back(local_column_index);
	}
	reader_data.empty_columns = reader_data.column_ids.empty();

	// Finally, initialize the filters
	bind_data.multi_file_reader->CreateFilterMap(bind_data.types, table_filters, reader_data, reader_state);
	reader_data.filters = table_filters;
}

static bool GetBooleanArgument(const pair<string, vector<Value>> &option) {
	if (option.second.empty()) {
		return true;
	}
	Value boolean_value;
	string error_message;
	if (!option.second[0].DefaultTryCastAs(LogicalType::BOOLEAN, boolean_value, &error_message)) {
		throw InvalidInputException("Unable to cast \"%s\" to BOOLEAN for Parquet option \"%s\"",
		                            option.second[0].ToString(), option.first);
	}
	return BooleanValue::Get(boolean_value);
}

class ParquetScanFunctionOverridden {
public:
	static TableFunctionSet GetFunctionSet() {
		TableFunction table_function("parquet_scan", {LogicalType::VARCHAR}, ParquetScanImplementation, ParquetScanBind,
		                             ParquetScanInitGlobal, ParquetScanInitLocal);
		table_function.statistics = ParquetScanStats;
		table_function.cardinality = ParquetCardinality;
		table_function.table_scan_progress = ParquetProgress;
		table_function.named_parameters["binary_as_string"] = LogicalType::BOOLEAN;
		table_function.named_parameters["file_row_number"] = LogicalType::BOOLEAN;
		table_function.named_parameters["compression"] = LogicalType::VARCHAR;
		table_function.named_parameters["schema"] =
		    LogicalType::MAP(LogicalType::INTEGER, LogicalType::STRUCT({{{"name", LogicalType::VARCHAR},
		                                                                 {"type", LogicalType::VARCHAR},
		                                                                 {"default_value", LogicalType::VARCHAR}}}));
		table_function.named_parameters["encryption_config"] = LogicalTypeId::ANY;
		table_function.get_batch_index = ParquetScanGetBatchIndex;
		table_function.serialize = ParquetScanSerialize;
		table_function.deserialize = ParquetScanDeserialize;
		table_function.get_bind_info = ParquetGetBindInfo;
		table_function.projection_pushdown = true;
		table_function.filter_pushdown = true;
		table_function.filter_prune = true;
		table_function.pushdown_complex_filter = ParquetComplexFilterPushdown;

		MultiFileReader::AddParameters(table_function);

		return MultiFileReader::CreateFunctionSet(table_function);
	}

	static unique_ptr<FunctionData> ParquetReadBind(ClientContext &context, CopyInfo &info,
	                                                vector<string> &expected_names,
	                                                vector<LogicalType> &expected_types) {
		D_ASSERT(expected_names.size() == expected_types.size());
		ParquetOptions parquet_options(context);

		for (auto &option : info.options) {
			auto loption = StringUtil::Lower(option.first);
			if (loption == "compression" || loption == "codec" || loption == "row_group_size") {
				// CODEC/COMPRESSION and ROW_GROUP_SIZE options have no effect on parquet read.
				// These options are determined from the file.
				continue;
			} else if (loption == "binary_as_string") {
				parquet_options.binary_as_string = GetBooleanArgument(option);
			} else if (loption == "file_row_number") {
				parquet_options.file_row_number = GetBooleanArgument(option);
			} else if (loption == "encryption_config") {
				if (option.second.size() != 1) {
					throw BinderException("Parquet encryption_config cannot be empty!");
				}
				parquet_options.encryption_config = ParquetEncryptionConfig::Create(context, option.second[0]);
			} else {
				throw NotImplementedException("Unsupported option for COPY FROM parquet: %s", option.first);
			}
		}

		// TODO: Allow overriding the MultiFileReader for COPY FROM?
		auto multi_file_reader = MultiFileReader::CreateDefault("ParquetCopy");
		vector<string> paths = {info.file_path};
		auto file_list = multi_file_reader->CreateFileList(context, paths);

		return ParquetScanBindInternal(context, std::move(multi_file_reader), std::move(file_list), expected_types,
		                               expected_names, parquet_options);
	}

	static unique_ptr<BaseStatistics> ParquetScanStats(ClientContext &context, const FunctionData *bind_data_p,
	                                                   column_t column_index) {
		auto &bind_data = bind_data_p->Cast<ParquetReadBindData>();

		if (IsRowIdColumnId(column_index)) {
			return nullptr;
		}

		// NOTE: we do not want to parse the Parquet metadata for the sole purpose of getting column statistics

		auto &config = DBConfig::GetConfig(context);

		if (bind_data.file_list->GetExpandResult() != FileExpandResult::MULTIPLE_FILES) {
			if (bind_data.initial_reader) {
				// most common path, scanning single parquet file
				return bind_data.initial_reader->ReadStatistics(bind_data.names[column_index]);
			} else if (!config.options.object_cache_enable) {
				// our initial reader was reset
				return nullptr;
			}
		} else if (config.options.object_cache_enable) {
			// multiple files, object cache enabled: merge statistics
			unique_ptr<BaseStatistics> overall_stats;

			auto &cache = ObjectCache::GetObjectCache(context);
			// for more than one file, we could be lucky and metadata for *every* file is in the object cache (if
			// enabled at all)
			FileSystem &fs = FileSystem::GetFileSystem(context);

			for (const auto &file_name : bind_data.file_list->Files()) {
				auto metadata = cache.Get<ParquetFileMetadataCache>(file_name);
				if (!metadata) {
					// missing metadata entry in cache, no usable stats
					return nullptr;
				}
				if (!fs.IsRemoteFile(file_name)) {
					auto handle = fs.OpenFile(file_name, FileFlags::FILE_FLAGS_READ);
					// we need to check if the metadata cache entries are current
					if (fs.GetLastModifiedTime(*handle) >= metadata->read_time) {
						// missing or invalid metadata entry in cache, no usable stats overall
						return nullptr;
					}
				} else {
					// for remote files we just avoid reading stats entirely
					return nullptr;
				}
				ParquetReader reader(context, bind_data.parquet_options, metadata);
				// get and merge stats for file
				auto file_stats = reader.ReadStatistics(bind_data.names[column_index]);
				if (!file_stats) {
					return nullptr;
				}
				if (overall_stats) {
					overall_stats->Merge(*file_stats);
				} else {
					overall_stats = std::move(file_stats);
				}
			}
			// success!
			return overall_stats;
		}

		// multiple files and no object cache, no luck!
		return nullptr;
	}

	static unique_ptr<FunctionData> ParquetScanBindInternal(ClientContext &context,
	                                                        unique_ptr<MultiFileReader> multi_file_reader,
	                                                        unique_ptr<MultiFileList> file_list,
	                                                        vector<LogicalType> &return_types, vector<string> &names,
	                                                        ParquetOptions parquet_options) {
		auto result = make_uniq<ParquetReadBindData>();
		result->multi_file_reader = std::move(multi_file_reader);
		result->file_list = std::move(file_list);

		bool bound_on_first_file = true;
		if (result->multi_file_reader->Bind(parquet_options.file_options, *result->file_list, result->types,
		                                    result->names, result->reader_bind)) {
			result->multi_file_reader->BindOptions(parquet_options.file_options, *result->file_list, result->types,
			                                       result->names, result->reader_bind);
			// Enable the parquet file_row_number on the parquet options if the file_row_number_idx was set
			if (result->reader_bind.file_row_number_idx != DConstants::INVALID_INDEX) {
				parquet_options.file_row_number = true;
			}
			bound_on_first_file = false;
		} else if (!parquet_options.schema.empty()) {
			// A schema was supplied: use the schema for binding
			result->reader_bind = BindSchema(context, result->types, result->names, *result, parquet_options);
		} else {
			parquet_options.file_options.AutoDetectHivePartitioning(*result->file_list, context);
			// Default bind
			result->reader_bind = result->multi_file_reader->BindReader<ParquetReader>(
			    context, result->types, result->names, *result->file_list, *result, parquet_options);
		}

		if (return_types.empty()) {
			// no expected types - just copy the types
			return_types = result->types;
			names = result->names;
		} else {
			if (return_types.size() != result->types.size()) {
				auto file_string = bound_on_first_file ? result->file_list->GetFirstFile()
				                                       : StringUtil::Join(result->file_list->GetPaths(), ",");
				throw std::runtime_error(StringUtil::Format(
				    "Failed to read file(s) \"%s\" - column count mismatch: expected %d columns but found %d",
				    file_string, return_types.size(), result->types.size()));
			}
			// expected types - overwrite the types we want to read instead
			result->types = return_types;
		}
		result->parquet_options = parquet_options;
		return std::move(result);
	}

	static unique_ptr<FunctionData> ParquetScanBind(ClientContext &context, TableFunctionBindInput &input,
	                                                vector<LogicalType> &return_types, vector<string> &names) {
		auto multi_file_reader = MultiFileReader::Create(input.table_function);

		ParquetOptions parquet_options(context);
		for (auto &kv : input.named_parameters) {
			auto loption = StringUtil::Lower(kv.first);
			if (multi_file_reader->ParseOption(kv.first, kv.second, parquet_options.file_options, context)) {
				continue;
			}
			if (loption == "binary_as_string") {
				parquet_options.binary_as_string = BooleanValue::Get(kv.second);
			} else if (loption == "file_row_number") {
				parquet_options.file_row_number = BooleanValue::Get(kv.second);
			} else if (loption == "schema") {
				// Argument is a map that defines the schema
				const auto &schema_value = kv.second;
				const auto column_values = ListValue::GetChildren(schema_value);
				if (column_values.empty()) {
					throw BinderException("Parquet schema cannot be empty");
				}
				parquet_options.schema.reserve(column_values.size());
				for (idx_t i = 0; i < column_values.size(); i++) {
					parquet_options.schema.emplace_back(
					    ParquetColumnDefinition::FromSchemaValue(context, column_values[i]));
				}

				// cannot be combined with hive_partitioning=true, so we disable auto-detection
				parquet_options.file_options.auto_detect_hive_partitioning = false;
			} else if (loption == "encryption_config") {
				parquet_options.encryption_config = ParquetEncryptionConfig::Create(context, kv.second);
			}
		}

		auto file_list = multi_file_reader->CreateFileList(context, input.inputs[0]);
		return ParquetScanBindInternal(context, std::move(multi_file_reader), std::move(file_list), return_types, names,
		                               parquet_options);
	}

	static double ParquetProgress(ClientContext &context, const FunctionData *bind_data_p,
	                              const GlobalTableFunctionState *global_state) {
		auto &bind_data = bind_data_p->Cast<ParquetReadBindData>();
		auto &gstate = global_state->Cast<ParquetReadGlobalState>();

		auto total_count = bind_data.file_list->GetTotalFileCount();
		if (total_count == 0) {
			return 100.0;
		}
		if (bind_data.initial_file_cardinality == 0) {
			return (100.0 * (gstate.file_index + 1)) / total_count;
		}
		auto percentage = MinValue<double>(
		    100.0, (bind_data.chunk_count * STANDARD_VECTOR_SIZE * 100.0 / bind_data.initial_file_cardinality));
		return (percentage + 100.0 * gstate.file_index) / total_count;
	}

	static unique_ptr<LocalTableFunctionState>
	ParquetScanInitLocal(ExecutionContext &context, TableFunctionInitInput &input, GlobalTableFunctionState *gstate_p) {
		auto &bind_data = input.bind_data->Cast<ParquetReadBindData>();
		auto &gstate = gstate_p->Cast<ParquetReadGlobalState>();

		auto result = make_uniq<ParquetReadLocalState>();
		result->is_parallel = true;
		result->batch_index = 0;

		if (gstate.CanRemoveColumns()) {
			result->all_columns.Initialize(context.client, gstate.scanned_types);
		}
		if (!ParquetParallelStateNext(context.client, bind_data, *result, gstate)) {
			return nullptr;
		}
		return std::move(result);
	}

	static unique_ptr<GlobalTableFunctionState> ParquetScanInitGlobal(ClientContext &context,
	                                                                  TableFunctionInitInput &input) {
		auto &bind_data = input.bind_data->CastNoConst<ParquetReadBindData>();
		auto result = make_uniq<ParquetReadGlobalState>();
		bind_data.file_list->InitializeScan(result->file_list_scan);

		result->multi_file_reader_state = bind_data.multi_file_reader->InitializeGlobalState(
		    context, bind_data.parquet_options.file_options, bind_data.reader_bind, *bind_data.file_list,
		    bind_data.types, bind_data.names, input.column_ids);
		if (bind_data.file_list->IsEmpty()) {
			result->readers = {};
		} else if (!bind_data.union_readers.empty()) {
			// TODO: confirm we are not changing behaviour by modifying the order here?
			for (auto &reader : bind_data.union_readers) {
				if (!reader) {
					break;
				}
				result->readers.push_back(ParquetFileReaderData(std::move(reader)));
			}
			if (result->readers.size() != bind_data.file_list->GetTotalFileCount()) {
				// This case happens with recursive CTEs: the first execution the readers have already
				// been moved out of the bind data.
				// FIXME: clean up this process and make it more explicit
				result->readers = {};
			}
		} else if (bind_data.initial_reader) {
			// Ensure the initial reader was actually constructed from the first file
			if (bind_data.initial_reader->file_name != bind_data.file_list->GetFirstFile()) {
				throw InternalException("First file from list ('%s') does not match first reader ('%s')",
				                        bind_data.initial_reader->file_name, bind_data.file_list->GetFirstFile());
			}
			result->readers.emplace_back(std::move(bind_data.initial_reader));
		}

		// Ensure all readers are initialized and FileListScan is sync with readers list
		for (auto &reader_data : result->readers) {
			string file_name;
			idx_t file_idx = result->file_list_scan.current_file_idx;
			bind_data.file_list->Scan(result->file_list_scan, file_name);
			if (file_name != reader_data.reader->file_name) {
				throw InternalException("Mismatch in filename order and reader order in parquet scan");
			}
			InitializeParquetReader(*reader_data.reader, bind_data, input.column_ids, input.filters, context, file_idx,
			                        result->multi_file_reader_state);
		}

		result->column_ids = input.column_ids;
		result->filters = input.filters.get();
		result->row_group_index = 0;
		result->file_index = 0;
		result->batch_index = 0;
		result->max_threads = ParquetScanMaxThreads(context, input.bind_data.get());

		bool require_extra_columns =
		    result->multi_file_reader_state && result->multi_file_reader_state->RequiresExtraColumns();
		if (input.CanRemoveFilterColumns() || require_extra_columns) {
            if (!input.projection_ids.empty()) {
                result->projection_ids = input.projection_ids;
            } else {
                result->projection_ids.resize(input.column_ids.size());
                iota(begin(result->projection_ids), end(result->projection_ids), 0);
            }
			const auto table_types = bind_data.types;
			for (const auto &col_idx : input.column_ids) {
				if (IsRowIdColumnId(col_idx)) {
					result->scanned_types.emplace_back(LogicalType::ROW_TYPE);
				} else {
					result->scanned_types.push_back(table_types[col_idx]);
				}
			}
		}

		if (require_extra_columns) {
			for (const auto &column_type : result->multi_file_reader_state->extra_columns) {
				result->scanned_types.push_back(column_type);
			}
		}

		return std::move(result);
	}

	static idx_t ParquetScanGetBatchIndex(ClientContext &context, const FunctionData *bind_data_p,
	                                      LocalTableFunctionState *local_state,
	                                      GlobalTableFunctionState *global_state) {
		auto &data = local_state->Cast<ParquetReadLocalState>();
		return data.batch_index;
	}

	static void ParquetScanSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
	                                 const TableFunction &function) {
		auto &bind_data = bind_data_p->Cast<ParquetReadBindData>();

		serializer.WriteProperty(100, "files", bind_data.file_list->GetAllFiles());
		serializer.WriteProperty(101, "types", bind_data.types);
		serializer.WriteProperty(102, "names", bind_data.names);
		serializer.WriteProperty(103, "parquet_options", bind_data.parquet_options);
	}

	static unique_ptr<FunctionData> ParquetScanDeserialize(Deserializer &deserializer, TableFunction &function) {
		auto &context = deserializer.Get<ClientContext &>();
		auto files = deserializer.ReadProperty<vector<string>>(100, "files");
		auto types = deserializer.ReadProperty<vector<LogicalType>>(101, "types");
		auto names = deserializer.ReadProperty<vector<string>>(102, "names");
		auto parquet_options = deserializer.ReadProperty<ParquetOptions>(103, "parquet_options");

		vector<Value> file_path;
		for (auto &path : files) {
			file_path.emplace_back(path);
		}

		auto multi_file_reader = MultiFileReader::Create(function);
		auto file_list = multi_file_reader->CreateFileList(context, Value::LIST(LogicalType::VARCHAR, file_path),
		                                                   FileGlobOptions::DISALLOW_EMPTY);
		return ParquetScanBindInternal(context, std::move(multi_file_reader), std::move(file_list), types, names,
		                               parquet_options);
	}

	static void ParquetScanImplementation(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
		if (!data_p.local_state) {
			return;
		}
		auto &data = data_p.local_state->Cast<ParquetReadLocalState>();
		auto &gstate = data_p.global_state->Cast<ParquetReadGlobalState>();
		auto &bind_data = data_p.bind_data->CastNoConst<ParquetReadBindData>();

		do {
			if (gstate.CanRemoveColumns()) {
				data.all_columns.Reset();
				data.reader->Scan(data.scan_state, data.all_columns);
				bind_data.multi_file_reader->FinalizeChunk(context, bind_data.reader_bind, data.reader->reader_data,
				                                           data.all_columns, gstate.multi_file_reader_state);
				output.ReferenceColumns(data.all_columns, gstate.projection_ids);
			} else {
				data.reader->Scan(data.scan_state, output);
				bind_data.multi_file_reader->FinalizeChunk(context, bind_data.reader_bind, data.reader->reader_data,
				                                           output, gstate.multi_file_reader_state);
			}

			bind_data.chunk_count++;
			if (output.size() > 0) {
				return;
			}
			if (!ParquetParallelStateNext(context, bind_data, data, gstate)) {
				return;
			}
		} while (true);
	}

	static unique_ptr<NodeStatistics> ParquetCardinality(ClientContext &context, const FunctionData *bind_data) {
		auto &data = bind_data->Cast<ParquetReadBindData>();
		return make_uniq<NodeStatistics>(data.initial_file_cardinality * data.file_list->GetTotalFileCount());
	}

	static idx_t ParquetScanMaxThreads(ClientContext &context, const FunctionData *bind_data) {
		auto &data = bind_data->Cast<ParquetReadBindData>();

		if (data.file_list->GetExpandResult() == FileExpandResult::MULTIPLE_FILES) {
			return TaskScheduler::GetScheduler(context).NumberOfThreads();
		}

		return MaxValue(data.initial_file_row_groups, (idx_t)1);
	}

	// Queries the metadataprovider for another file to scan, updating the files/reader lists in the process.
	// Returns true if resized
	static bool ResizeFiles(const ParquetReadBindData &bind_data, ParquetReadGlobalState &parallel_state) {
		string scanned_file;
		if (!bind_data.file_list->Scan(parallel_state.file_list_scan, scanned_file)) {
			return false;
		}

		// Push the file in the reader data, to be opened later
		parallel_state.readers.emplace_back(scanned_file);

		return true;
	}

	// This function looks for the next available row group. If not available, it will open files from bind_data.files
	// until there is a row group available for scanning or the files runs out
	static bool ParquetParallelStateNext(ClientContext &context, const ParquetReadBindData &bind_data,
	                                     ParquetReadLocalState &scan_data, ParquetReadGlobalState &parallel_state) {
		unique_lock<mutex> parallel_lock(parallel_state.lock);

		while (true) {
			if (parallel_state.error_opening_file) {
				return false;
			}

			if (parallel_state.file_index >= parallel_state.readers.size() && !ResizeFiles(bind_data, parallel_state)) {
				return false;
			}

			auto &current_reader_data = parallel_state.readers[parallel_state.file_index];
			if (current_reader_data.file_state == ParquetFileState::OPEN) {
				if (parallel_state.row_group_index < current_reader_data.reader->NumRowGroups()) {
					// The current reader has rowgroups left to be scanned
					scan_data.reader = current_reader_data.reader;
					vector<idx_t> group_indexes {parallel_state.row_group_index};
					scan_data.reader->InitializeScan(scan_data.scan_state, group_indexes);
					scan_data.batch_index = parallel_state.batch_index++;
					scan_data.file_index = parallel_state.file_index;
					parallel_state.row_group_index++;
					return true;
				} else {
					// Close current file
					current_reader_data.file_state = ParquetFileState::CLOSED;
					current_reader_data.reader = nullptr;

					// Set state to the next file
					parallel_state.file_index++;
					parallel_state.row_group_index = 0;

					continue;
				}
			}

			if (TryOpenNextFile(context, bind_data, scan_data, parallel_state, parallel_lock)) {
				continue;
			}

			// Check if the current file is being opened, in that case we need to wait for it.
			if (parallel_state.readers[parallel_state.file_index].file_state == ParquetFileState::OPENING) {
				WaitForFile(parallel_state.file_index, parallel_state, parallel_lock);
			}
		}
	}

	static void ParquetComplexFilterPushdown(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
	                                         vector<unique_ptr<Expression>> &filters) {
		auto &data = bind_data_p->Cast<ParquetReadBindData>();

		auto new_list = data.multi_file_reader->ComplexFilterPushdown(context, *data.file_list,
		                                                              data.parquet_options.file_options, get, filters);

		if (new_list) {
			data.file_list = std::move(new_list);
			MultiFileReader::PruneReaders(data, *data.file_list);
		}
	}

	//! Wait for a file to become available. Parallel lock should be locked when calling.
	static void WaitForFile(idx_t file_index, ParquetReadGlobalState &parallel_state,
	                        unique_lock<mutex> &parallel_lock) {
		while (true) {

			// Get pointer to file mutex before unlocking
			auto &file_mutex = *parallel_state.readers[file_index].file_mutex;

			// To get the file lock, we first need to release the parallel_lock to prevent deadlocking. Note that this
			// requires getting the ref to the file mutex pointer with the lock stil held: readers get be resized
			parallel_lock.unlock();
			unique_lock<mutex> current_file_lock(file_mutex);
			parallel_lock.lock();

			// Here we have both locks which means we can stop waiting if:
			// - the thread opening the file is done and the file is available
			// - the thread opening the file has failed
			// - the file was somehow scanned till the end while we were waiting
			if (parallel_state.file_index >= parallel_state.readers.size() ||
			    parallel_state.readers[parallel_state.file_index].file_state != ParquetFileState::OPENING ||
			    parallel_state.error_opening_file) {
				return;
			}
		}
	}

	//! Helper function that try to start opening a next file. Parallel lock should be locked when calling.
	static bool TryOpenNextFile(ClientContext &context, const ParquetReadBindData &bind_data,
	                            ParquetReadLocalState &scan_data, ParquetReadGlobalState &parallel_state,
	                            unique_lock<mutex> &parallel_lock) {
		const auto num_threads = TaskScheduler::GetScheduler(context).NumberOfThreads();

		const auto file_index_limit =
		    MinValue<idx_t>(parallel_state.file_index + num_threads, parallel_state.readers.size());

		for (idx_t i = parallel_state.file_index; i < file_index_limit; i++) {
			if (parallel_state.readers[i].file_state == ParquetFileState::UNOPENED) {
				auto &current_reader_data = parallel_state.readers[i];
				current_reader_data.file_state = ParquetFileState::OPENING;
				auto pq_options = bind_data.parquet_options;

				// Get pointer to file mutex before unlocking
				auto &current_file_lock = *current_reader_data.file_mutex;

				// Now we switch which lock we are holding, instead of locking the global state, we grab the lock on
				// the file we are opening. This file lock allows threads to wait for a file to be opened.
				parallel_lock.unlock();
				unique_lock<mutex> file_lock(current_file_lock);

				shared_ptr<ParquetReader> reader;
				try {
					reader = make_shared_ptr<ParquetReader>(context, current_reader_data.file_to_be_opened, pq_options);
					InitializeParquetReader(*reader, bind_data, parallel_state.column_ids, parallel_state.filters,
					                        context, i, parallel_state.multi_file_reader_state);
				} catch (...) {
					parallel_lock.lock();
					parallel_state.error_opening_file = true;
					throw;
				}

				// Now re-lock the state and add the reader
				parallel_lock.lock();
				current_reader_data.reader = reader;
				current_reader_data.file_state = ParquetFileState::OPEN;

				return true;
			}
		}

		return false;
	}
};

TableFunctionSet ParquetOverrideFunction::GetFunctionSet() {
    auto scan_fun = ParquetScanFunctionOverridden::GetFunctionSet();
    return scan_fun;
}

} // namespace duckdb

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
