# Overridden Parquet Scan
This exists because some upstream changes in the parquet reader are required right now.


## Changes made
### Fix extra columns not allowed for count(*) queries

In the parquet_extension.cpp `ParquetInitGlobalMethod`
We changed:
```c++
result->projection_ids = input.projection_ids;
```
into 
```c++
 if (!input.projection_ids.empty()) {
    result->projection_ids = input.projection_ids;
} else {
    result->projection_ids.resize(input.column_ids.size());
    iota(begin(result->projection_ids), end(result->projection_ids), 0);
}
```
to esnure the projections ids are set even if they are trivial: in the case where 
the delta extension has added extra columns, we need the trivial projection ids
to ensure the extra columns are removed before leaving the scan