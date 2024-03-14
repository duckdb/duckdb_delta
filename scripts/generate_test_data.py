from deltalake import DeltaTable, write_deltalake
import duckdb
import pandas as pd
import os
import shutil

BASE_PATH = "./data/generated/"

def delete_old_files():
    if (os.path.isdir(BASE_PATH)):
        shutil.rmtree(BASE_PATH)
def generate_test_data(path, query, part_column=False):
    """
    generate_test_data generates some test data using delta-rs and duckdb

    :param path: the test data path (prefixed with: './data/generated/')
    :param query: a duckdb query that produces a table called 'test_table'
    :param part_column: Optionally the name of the column to partition by
    :return: describe what it returns
    """
    generated_path = f"{BASE_PATH}/{path}"

    con = duckdb.connect()

    con.sql(query)

    # Write delta table data
    test_table_df = con.sql("FROM test_table;").df()
    if (part_column):
        write_deltalake(f"{generated_path}/delta_lake", test_table_df,  partition_by=[part_column])
    else:
        write_deltalake(f"{generated_path}/delta_lake", test_table_df)

    # Write DuckDB's reference data
    os.mkdir(f'{generated_path}/duckdb')
    if (part_column):
        con.sql(f"COPY test_table to '{generated_path}/duckdb' (FORMAT parquet, PARTITION_BY {part_column})")
    else:
        con.sql(f"COPY test_table to '{generated_path}/duckdb/data.parquet' (FORMAT parquet)")

delete_old_files()

### Lineitem SF0.01 No partitions
query = "call dbgen(sf=0.01);"
query += "CREATE table test_table AS SELECT * as part from lineitem;"
generate_test_data("lineitem_sf0_01", query)

### Lineitem SF0.01 10 Partitions
query = "call dbgen(sf=0.01);"
query += "CREATE table test_table AS SELECT *, l_orderkey%10 as part from lineitem;"
generate_test_data("lineitem_sf0_01_10part", query, "part")

### Lineitem SF1 10 Partitions
query = "call dbgen(sf=1);"
query += "CREATE table test_table AS SELECT *, l_orderkey%10 as part from lineitem;"
generate_test_data("lineitem_sf1_10part", query, "part")