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

ffi::EngineError* allocator_error_fun(ffi::KernelError etype, ffi::KernelStringSlice msg) {
      printf("Error thingy: %s", string(msg.ptr, msg.len).c_str());
      return nullptr;
};


} // namespace duckdb
