#pragma once

#include "deltakernel-ffi.hpp"

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

class PredicateVisitor : public ffi::EnginePredicate {
    public:
        PredicateVisitor(const vector<string> &column_names, optional_ptr<TableFilterSet> filters) : EnginePredicate{
                                                                                                       .predicate = this,
                                                                                                       .visitor = (uintptr_t (*)(void *, ffi::KernelExpressionVisitorState *)) &VisitPredicate} {
        }
    private:
      static uintptr_t VisitPredicate(PredicateVisitor* predicate, ffi::KernelExpressionVisitorState* state) {
          return 0;
      }
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

ffi::KernelStringSlice str_slice(const string &str) {
      return {str.data(), str.size()};
}

struct DuckDBEngineError : ffi::EngineError {
    string error_message;
};

ffi::EngineError* error_allocator(ffi::KernelError etype, ffi::KernelStringSlice msg) {
    auto error = new DuckDBEngineError;
    error->etype = etype;
    error->error_message = string(msg.ptr, msg.len);
    return error;
};

// TODO make less hacky
static const char* KernelErrorEnumStrings[] = {
        "UnknownError",
        "FFIError",
        "ArrowError",
        "GenericError",
        "ParquetError",
        "ObjectStoreError",
        "FileNotFoundError",
        "MissingColumnError",
        "UnexpectedColumnTypeError",
        "MissingDataError",
        "MissingVersionError",
        "DeletionVectorError",
        "InvalidUrlError",
        "MalformedJsonError",
        "MissingMetadataError"
};
static_assert(sizeof(KernelErrorEnumStrings)/sizeof(char*)-1 == (int)ffi::KernelError::MissingMetadataError,
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


} // namespace duckdb
