from deltalake import DeltaTable, write_deltalake
from pyspark.sql import SparkSession
from delta import *
from pyspark.sql.functions import *
import duckdb
import pandas as pd
import os
import shutil
import math
import glob

BASE_PATH = os.path.dirname(os.path.realpath(__file__)) + "/../data/generated"
TMP_PATH = '/tmp'

def delete_old_files():
    if (os.path.isdir(BASE_PATH)):
        shutil.rmtree(BASE_PATH)

def generate_test_data_delta_rs_multi(path, init, tables, splits = 1):
    """
    generate_test_data_delta_rs generates some test data using delta-rs and duckdb

    :param path: the test data path (prefixed with BASE_PATH)
    :param init: a duckdb query initializes the duckdb tables that will be written
    :param tables: list of dicts containing the fields: name, query, (optionally) part_column
    :return: describe what it returns
    """
    generated_path = f"{BASE_PATH}/{path}"

    if (os.path.isdir(generated_path)):
        return

    os.makedirs(f"{generated_path}")

    # First we write a DuckDB file TODO: this should go in N appends as well?
    con = duckdb.connect(f"{generated_path}/duckdb.db")

    con.sql(init)

    # Then we write the parquet files
    for table in tables:
        total_count = con.sql(f"select count(*) from ({table['query']})").fetchall()[0][0]
        # At least 1 tuple per file
        if total_count < splits:
            splits = total_count
        tuples_per_file = total_count // splits
        remainder = total_count % splits

        file_no = 0
        write_from = 0
        while file_no < splits:
            os.makedirs(f"{generated_path}/{table['name']}/parquet", exist_ok=True)
        # Write DuckDB's reference data
            write_to = write_from + tuples_per_file + (1 if file_no < remainder else 0)
            con.sql(f"COPY ({table['query']} where rowid >= {write_from} and rowid < {write_to}) to '{generated_path}/{table['name']}/parquet/data_{file_no}.parquet' (FORMAT parquet)")
            file_no += 1
            write_from = write_to

    for table in tables:
        con = duckdb.connect(f"{generated_path}/duckdb.db")
        file_list = list(glob.glob(f"{generated_path}/{table['name']}/parquet/*.parquet"))
        file_list = sorted(file_list)
        for file in file_list:
            test_table_df = con.sql(f'from "{file}"').arrow()
            os.makedirs(f"{generated_path}/{table['name']}/delta_lake", exist_ok=True)
            write_deltalake(f"{generated_path}/{table['name']}/delta_lake", test_table_df, mode="append")

def generate_test_data_delta_rs(path, query, part_column=False, add_golden_table=True):
    """
    generate_test_data_delta_rs generates some test data using delta-rs and duckdb

    :param path: the test data path (prefixed with BASE_PATH)
    :param query: a duckdb query that produces a table called 'test_table'
    :param part_column: Optionally the name of the column to partition by
    :return: describe what it returns
    """
    generated_path = f"{BASE_PATH}/{path}"

    if (os.path.isdir(generated_path)):
        return

    con = duckdb.connect()

    con.sql(query)

    # Write delta table data
    test_table_df = con.sql("FROM test_table;").df()
    if (part_column):
        write_deltalake(f"{generated_path}/delta_lake", test_table_df,  partition_by=[part_column])
    else:
        write_deltalake(f"{generated_path}/delta_lake", test_table_df)

    if add_golden_table:
        # Write DuckDB's reference data
        os.mkdir(f'{generated_path}/duckdb')
        if (part_column):
            con.sql(f"COPY test_table to '{generated_path}/duckdb' (FORMAT parquet, PARTITION_BY {part_column})")
        else:
            con.sql(f"COPY test_table to '{generated_path}/duckdb/data.parquet' (FORMAT parquet)")

def generate_test_data_pyspark(name, current_path, input_path, delete_predicate = False):
    """
    generate_test_data_pyspark generates some test data using pyspark and duckdb

    :param current_path: the test data path
    :param input_path: the path to an input parquet file
    :return: describe what it returns
    """

    if (os.path.isdir(BASE_PATH + '/' + current_path)):
        return

    ## SPARK SESSION
    builder = SparkSession.builder.appName("MyApp") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.memory", "8g")\
        .config('spark.driver.host','127.0.0.1')

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    ## CONFIG
    delta_table_path = BASE_PATH + '/' + current_path + '/delta_lake'
    parquet_reference_path = BASE_PATH + '/' + current_path + '/parquet'

    ## CREATE DIRS
    os.makedirs(delta_table_path, exist_ok=True)
    os.makedirs(parquet_reference_path, exist_ok=True)

    ## DATA GENERATION
    # df = spark.read.parquet(input_path)
    # df.write.format("delta").mode("overwrite").save(delta_table_path)
    spark.sql(f"CREATE TABLE test_table_{name} USING delta LOCATION '{delta_table_path}' AS SELECT * FROM parquet.`{input_path}`")

    ## CREATE
    ## CONFIGURE USAGE OF DELETION VECTORS
    if (delete_predicate):
        spark.sql(f"ALTER TABLE test_table_{name} SET TBLPROPERTIES ('delta.enableDeletionVectors' = true);")

    ## ADDING DELETES
    deltaTable = DeltaTable.forPath(spark, delta_table_path)
    if delete_predicate:
        deltaTable.delete(delete_predicate)

    ## WRITING THE PARQUET FILES
    df = spark.table(f'test_table_{name}')
    df.write.parquet(parquet_reference_path, mode='overwrite')

# TO CLEAN, uncomment
# delete_old_files()

### TPCH SF1
init = "call dbgen(sf=0.01);"
tables = ["customer","lineitem","nation","orders","part","partsupp","region","supplier"]
queries = [f"from {x}" for x in tables]
tables = [{'name': x[0], 'query':x[1]} for x in zip(tables,queries)]
generate_test_data_delta_rs_multi("delta_rs_tpch_sf0_01", init, tables)

### Simple partitioned table
query = "CREATE table test_table AS SELECT i, i%2 as part from range(0,10) tbl(i);"
generate_test_data_delta_rs("simple_partitioned", query, "part")

### Lineitem SF0.01 No partitions
query = "call dbgen(sf=0.01);"
query += "CREATE table test_table AS SELECT * as part from lineitem;"
generate_test_data_delta_rs("lineitem_sf0_01", query)

### Lineitem SF0.01 10 Partitions
query = "call dbgen(sf=0.01);"
query += "CREATE table test_table AS SELECT *, l_orderkey%10 as part from lineitem;"
generate_test_data_delta_rs("lineitem_sf0_01_10part", query, "part")

## Simple table with a blob as a value
query = "create table test_table as SELECT encode('ABCDE') as blob, encode('ABCDE') as blob_part, 'ABCDE' as string UNION ALL SELECT encode('😈') as blob, encode('😈') as blob_part, '😈' as string"
generate_test_data_delta_rs("simple_blob_table", query, "blob_part", add_golden_table=False)

## Simple partitioned table with structs
query = "CREATE table test_table AS SELECT {'i':i, 'j':i+1} as value, i%2 as part from range(0,10) tbl(i);"
generate_test_data_delta_rs("simple_partitioned_with_structs", query, "part")

## Partitioned table with all types we can file skip on
for type in ["bool", "int", "tinyint", "smallint", "bigint", "float", "double", "varchar"]:
    query = f"CREATE table test_table as select i::{type} as value, i::{type} as part from range(0,2) tbl(i)"
    generate_test_data_delta_rs(f"test_file_skipping/{type}", query, "part")

## Simple table with deletion vector
con = duckdb.connect()
con.query(f"COPY (SELECT i as id, ('val' || i::VARCHAR) as value  FROM range(0,1000000) tbl(i))TO '{TMP_PATH}/simple_sf1_with_dv.parquet'")
generate_test_data_pyspark('simple_sf1_with_dv', 'simple_sf1_with_dv', f'{TMP_PATH}/simple_sf1_with_dv.parquet', "id % 1000 = 0")

## Lineitem SF0.01 with deletion vector
con = duckdb.connect()
con.query(f"call dbgen(sf=0.01); COPY (from lineitem) TO '{TMP_PATH}/modified_lineitem_sf0_01.parquet'")
generate_test_data_pyspark('lineitem_sf0_01_with_dv', 'lineitem_sf0_01_with_dv', f'{TMP_PATH}/modified_lineitem_sf0_01.parquet', "l_shipdate = '1994-01-01'")

## Lineitem SF1 with deletion vector
con = duckdb.connect()
con.query(f"call dbgen(sf=1); COPY (from lineitem) TO '{TMP_PATH}/modified_lineitem_sf1.parquet'")
generate_test_data_pyspark('lineitem_sf1_with_dv', 'lineitem_sf1_with_dv', f'{TMP_PATH}/modified_lineitem_sf1.parquet', "l_shipdate = '1994-01-01'")

## TPCH SF0.01 full dataset
con = duckdb.connect()
con.query(f"call dbgen(sf=0.01); EXPORT DATABASE '{TMP_PATH}/tpch_sf0_01_export' (FORMAT parquet)")
for table in ["customer","lineitem","nation","orders","part","partsupp","region","supplier"]:
    generate_test_data_pyspark(f"tpch_sf0_01_{table}", f'tpch_sf0_01/{table}', f'{TMP_PATH}/tpch_sf0_01_export/{table}.parquet')

## TPCDS SF0.01 full dataset
con = duckdb.connect()
con.query(f"call dsdgen(sf=0.01); EXPORT DATABASE '{TMP_PATH}/tpcds_sf0_01_export' (FORMAT parquet)")
for table in ["call_center","catalog_page","catalog_returns","catalog_sales","customer","customer_demographics","customer_address","date_dim","household_demographics","inventory","income_band","item","promotion","reason","ship_mode","store","store_returns","store_sales","time_dim","warehouse","web_page","web_returns","web_sales","web_site"]:
    generate_test_data_pyspark(f"tpcds_sf0_01_{table}", f'tpcds_sf0_01/{table}', f'{TMP_PATH}/tpcds_sf0_01_export/{table}.parquet')

## TPCH SF1 full dataset
if (not os.path.isdir(BASE_PATH + '/tpch_sf1')):
    con = duckdb.connect()
    con.query(f"call dbgen(sf=1); EXPORT DATABASE '{TMP_PATH}/tpch_sf1_export' (FORMAT parquet)")
    for table in ["customer","lineitem","nation","orders","part","partsupp","region","supplier"]:
        generate_test_data_pyspark(f"tpch_sf1_{table}", f'tpch_sf1/{table}', f'{TMP_PATH}/tpch_sf1_export/{table}.parquet')
    con.query(f"attach '{BASE_PATH + '/tpch_sf1/duckdb.db'}' as duckdb_out")
    for table in ["customer","lineitem","nation","orders","part","partsupp","region","supplier"]:
        con.query(f"create table duckdb_out.{table} as from {table}")

## TPCDS SF1 full dataset
if (not os.path.isdir(BASE_PATH + '/tpcds_sf1')):
    con = duckdb.connect()
    con.query(f"call dsdgen(sf=1); EXPORT DATABASE '{TMP_PATH}/tpcds_sf1_export' (FORMAT parquet)")
    for table in ["call_center","catalog_page","catalog_returns","catalog_sales","customer","customer_demographics","customer_address","date_dim","household_demographics","inventory","income_band","item","promotion","reason","ship_mode","store","store_returns","store_sales","time_dim","warehouse","web_page","web_returns","web_sales","web_site"]:
        generate_test_data_pyspark(f"tpcds_sf1_{table}", f'tpcds_sf1/{table}', f'{TMP_PATH}/tpcds_sf1_export/{table}.parquet')
    con.query(f"attach '{BASE_PATH + '/tpcds_sf1/duckdb.db'}' as duckdb_out")
    for table in ["call_center","catalog_page","catalog_returns","catalog_sales","customer","customer_demographics","customer_address","date_dim","household_demographics","inventory","income_band","item","promotion","reason","ship_mode","store","store_returns","store_sales","time_dim","warehouse","web_page","web_returns","web_sales","web_site"]:
        con.query(f"create table duckdb_out.{table} as from {table}")