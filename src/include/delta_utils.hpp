#pragma once

#include "delta_kernel_ffi.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include <iostream>

// TODO: clean up this file as we go

namespace duckdb {

class SchemaVisitor {
public:
    using FieldList = child_list_t<LogicalType>;

    static unique_ptr<FieldList> VisitSnapshotSchema(const ffi::SnapshotHandle* snapshot) {
        SchemaVisitor state;
        ffi::EngineSchemaVisitor visitor = {
                .data = &state,
                .make_field_list = (uintptr_t (*)(void*, uintptr_t)) &MakeFieldList,
                .visit_struct = (void (*)(void*, uintptr_t, ffi::KernelStringSlice, uintptr_t)) &VisitStruct,
                .visit_string = VisitSimpleType<LogicalType::VARCHAR>(),
                .visit_integer = VisitSimpleType<LogicalType::INTEGER>(),
                .visit_long = VisitSimpleType<LogicalType::BIGINT>(),
        };
        uintptr_t result = visit_schema(snapshot, &visitor);
        return state.TakeFieldList(result);
    }

private:
    std::map<uintptr_t, unique_ptr<FieldList> > inflight_lists;
    uintptr_t next_id = 1;

    typedef void (SimpleTypeVisitorFunction)(void*, uintptr_t, ffi::KernelStringSlice);

    template <LogicalTypeId TypeId>
    static SimpleTypeVisitorFunction* VisitSimpleType() {
        return (SimpleTypeVisitorFunction*) &VisitSimpleTypeImpl<TypeId>;
    }
    template <LogicalTypeId TypeId>
    static void VisitSimpleTypeImpl(SchemaVisitor* state, uintptr_t sibling_list_id, ffi::KernelStringSlice name) {
        state->AppendToList(sibling_list_id, name, TypeId);
    }

    static uintptr_t MakeFieldList(SchemaVisitor* state, uintptr_t capacity_hint) {
        return state->MakeFieldListImpl(capacity_hint);
    }

    static void VisitStruct(SchemaVisitor* state, uintptr_t sibling_list_id, ffi::KernelStringSlice name, uintptr_t child_list_id) {
        auto children = state->TakeFieldList(child_list_id);
        state->AppendToList(sibling_list_id, name, LogicalType::STRUCT(std::move(*children)));
    }

    uintptr_t MakeFieldListImpl(uintptr_t capacity_hint) {
        uintptr_t id = next_id++;
        auto list = make_uniq<FieldList>();
        if (capacity_hint > 0) {
            list->reserve(capacity_hint);
        }
        inflight_lists.emplace(id, std::move(list));
        return id;
    }

    void AppendToList(uintptr_t id, ffi::KernelStringSlice name, LogicalType&& child) {
        auto it = inflight_lists.find(id);
        if (it == inflight_lists.end()) {
            // TODO... some error...
        } else {
            it->second->emplace_back(std::make_pair(string(name.ptr, name.len), std::move(child)));
        }
    }

    unique_ptr<FieldList> TakeFieldList(uintptr_t id) {
        auto it = inflight_lists.find(id);
        if (it == inflight_lists.end()) {
            // TODO: Raise some kind of error.
            return {}; // not present
        }
        auto rval = std::move(it->second);
        inflight_lists.erase(it);
        return rval;
    }
};

struct DuckDBEngineError : ffi::EngineError {
    string error_message;
};

ffi::EngineError* error_allocator(ffi::KernelError etype, ffi::KernelStringSlice msg) {
    auto error = new DuckDBEngineError;
    error->etype = etype;
    error->error_message = string(msg.ptr, msg.len);
    return error;
};

// RAII wrapper that returns ownership of a kernel pointer to kernel when it goes out of
// scope. Similar to std::unique_ptr. but does not define operator->() and does not require the
// kernel type to be complete.
template <typename KernelType>
struct UniqueKernelPointer {
    UniqueKernelPointer() : ptr(nullptr), free(nullptr) {}

    // Takes ownership of a pointer with associated deleter.
    UniqueKernelPointer(KernelType* ptr, void (*free)(KernelType*)) : ptr(ptr), free(free) {}

    // movable but not copyable
    UniqueKernelPointer(UniqueKernelPointer&& other) : ptr(other.ptr) {
        other.ptr = nullptr;
    }
    UniqueKernelPointer& operator=(UniqueKernelPointer&& other) {
        std::swap(ptr, other.ptr);
        std::swap(free, other.free);
        return *this;
    }
    UniqueKernelPointer(const UniqueKernelPointer&) = delete;
    UniqueKernelPointer& operator=(const UniqueKernelPointer&) = delete;

    ~UniqueKernelPointer() {
        if (ptr && free) {
            free(ptr);
        }
    }

    KernelType* get() const { return ptr; }

private:
    KernelType* ptr;
    void (*free)(KernelType*) = nullptr;
};

// TODO make less hacky
static const char* KernelErrorEnumStrings[] = {
        "UnknownError",
        "FFIError",
        "ArrowError",
        "EngineDataTypeError",
        "ExtractError",
        "GenericError",
        "IOErrorError",
        "ParquetError",
        "ObjectStoreError",
        "ObjectStorePathError",
        "FileNotFoundError",
        "MissingColumnError",
        "UnexpectedColumnTypeError",
        "MissingDataError",
        "MissingVersionError",
        "DeletionVectorError",
        "InvalidUrlError",
        "MalformedJsonError",
        "MissingMetadataError",
        "MissingProtocolError",
        "MissingMetadataAndProtocolError",
        "ParseError",
        "JoinFailureError",
        "Utf8Error",
        "ParseIntError"
};
static_assert(sizeof(KernelErrorEnumStrings)/sizeof(char*)-1 == (int)ffi::KernelError::ParseIntError,
              "KernelErrorEnumStrings failin");

static string kernel_error_to_string(ffi::KernelError err) {
    return KernelErrorEnumStrings[(int)err];
}

// TODO: not unpacking an ExternResult with an error will now lead to memory leak
template <class T>
static T unpack_result_or_throw(ffi::ExternResult<T> result, const string &from_where) {
    if (result.tag == ffi::ExternResult<T>::Tag::Err) {
        if (result.err._0){
            auto error_cast = static_cast<DuckDBEngineError*>(result.err._0);
            auto etype = error_cast->etype;
            auto message = error_cast->error_message;
            free(error_cast);

            throw InternalException("Hit DeltaKernel FFI error (from: %s): Hit error: %u (%s) with message (%s)",
                                    from_where.c_str(), etype, kernel_error_to_string(etype), message);
        } else {
            throw InternalException("Hit DeltaKernel FFI error (from: %s): Hit error, but error was nullptr", from_where.c_str());
        }
    } else if (result.tag == ffi::ExternResult<T>::Tag::Ok) {
        return result.ok._0;
    }
    throw InternalException("Invalid error ExternResult tag found!");
}

template <class T>
bool result_is_ok(ffi::ExternResult<T> result) {
    if (result.tag == ffi::ExternResult<T>::Tag::Ok) {
        return true;
    } else if (result.tag == ffi::ExternResult<T>::Tag::Err) {
        return false;
    }
    throw InternalException("Invalid error ExternResult tag found!");
}

ffi::KernelStringSlice to_delta_string_slice(const string &str) {
    return {str.data(), str.size()};
}

string from_delta_string_slice(const struct ffi::KernelStringSlice slice) {
    return {slice.ptr, slice.len};
}

vector<bool> from_delta_bool_slice(const struct ffi::KernelBoolSlice slice) {
    vector<bool> result;
    result.assign(slice.ptr, slice.ptr + slice.len);
    return result;
}

// Template wrapper function that implements get_next for EngineIteratorFromCallable.
template <typename Callable>
static auto GetNextFromCallable(Callable* callable) -> decltype(std::declval<Callable>()()) {
    return callable->operator()();
}

// Wraps a callable object (e.g. C++11 lambda) as an EngineIterator.
template <typename Callable>
ffi::EngineIterator EngineIteratorFromCallable(Callable& callable) {
    auto* get_next = &GetNextFromCallable<Callable>;
    return {.data = &callable, .get_next = (const void *(*)(void*)) get_next};
};

class PredicateVisitor : public ffi::EnginePredicate {
public:
    PredicateVisitor(const vector<string> &column_names, optional_ptr<TableFilterSet> filters) : EnginePredicate {
            .predicate = this,
            .visitor = (uintptr_t (*)(void*, ffi::KernelExpressionVisitorState*)) &VisitPredicate}
    {
        if (filters) {
            for (auto& filter : filters->filters) {
                column_filters[column_names[filter.first]] = filter.second.get();
            }
        }
    }

private:
    std::map<string, TableFilter*> column_filters;

    static uintptr_t VisitPredicate(PredicateVisitor* predicate, ffi::KernelExpressionVisitorState* state) {
        auto& filters = predicate->column_filters;
        auto it = filters.begin();
        auto end = filters.end();
        auto get_next = [predicate, state, &it, &end]() -> uintptr_t {
            if (it == end) {
                return 0;
            }
            auto &filter = *it++;
            return predicate->VisitFilter(filter.first, *filter.second, state);
        };
        auto eit = EngineIteratorFromCallable(get_next);
        return visit_expression_and(state, &eit);
    }

    uintptr_t VisitConstantFilter(const string &col_name, const ConstantFilter &filter, ffi::KernelExpressionVisitorState* state) {
        auto maybe_left = ffi::visit_expression_column(state, to_delta_string_slice(col_name), error_allocator);
        uintptr_t left = unpack_result_or_throw(maybe_left, "VisitConstantFilter failed to visit_expression_column");

        uintptr_t right = ~0;
        auto &value = filter.constant;
        switch (value.type().id()) {
            case LogicalType::BIGINT:
                right = visit_expression_literal_long(state, BigIntValue::Get(value));
                break;

            case LogicalType::VARCHAR: {
                // WARNING: C++ lifetime extension rules don't protect calls of the form foo(std::string(...).c_str())
                auto str = StringValue::Get(value);
                auto maybe_right = ffi::visit_expression_literal_string(state, to_delta_string_slice(col_name), error_allocator);
                right = unpack_result_or_throw(maybe_right, "VisitConstantFilter failed to visit_expression_literal_string");
                break;
            }

            default:
                break; // unsupported type
        }

        switch (filter.comparison_type) {
            case ExpressionType::COMPARE_LESSTHAN:
                return visit_expression_lt(state, left, right);
            case ExpressionType::COMPARE_LESSTHANOREQUALTO:
                return visit_expression_le(state, left, right);
            case ExpressionType::COMPARE_GREATERTHAN:
                return visit_expression_gt(state, left, right);
            case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
                return visit_expression_ge(state, left, right);
            case ExpressionType::COMPARE_EQUAL:
                return visit_expression_eq(state, left, right);

            default:
                std::cout << " Unsupported operation: " << (int) filter.comparison_type << std::endl;
                return ~0; // Unsupported operation
        }
    }

    uintptr_t VisitAndFilter(const string &col_name, const ConjunctionAndFilter &filter, ffi::KernelExpressionVisitorState* state) {
        auto it = filter.child_filters.begin();
        auto end = filter.child_filters.end();
        auto get_next = [this, col_name, state, &it, &end]() -> uintptr_t {
            if (it == end) {
                return 0;
            }
            auto &child_filter = *it++;
            return VisitFilter(col_name, *child_filter, state);
        };
        auto eit = EngineIteratorFromCallable(get_next);
        return visit_expression_and(state, &eit);
    }

    uintptr_t VisitFilter(const string &col_name, const TableFilter &filter, ffi::KernelExpressionVisitorState* state) {
        switch (filter.filter_type) {
            case TableFilterType::CONSTANT_COMPARISON:
                return VisitConstantFilter(col_name, static_cast<const ConstantFilter&>(filter), state);

            case TableFilterType::CONJUNCTION_AND:
                return VisitAndFilter(col_name, static_cast<const ConjunctionAndFilter&>(filter), state);

            default:
                return ~0; // Unsupported filter
        }
    }
};

} // namespace duckdb
