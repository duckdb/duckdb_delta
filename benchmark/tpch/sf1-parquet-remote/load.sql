create view customer as from parquet_scan('s3://test-bucket-ceiveran/delta_testing/tpch_sf0_01' || '/customer/parquet/**/*.parquet');
create view lineitem as from parquet_scan('s3://test-bucket-ceiveran/delta_testing/tpch_sf0_01' ||  '/lineitem/parquet/**/*.parquet');
create view nation as from parquet_scan('s3://test-bucket-ceiveran/delta_testing/tpch_sf0_01' || '/nation/parquet/**/*.parquet');
create view orders as from parquet_scan('s3://test-bucket-ceiveran/delta_testing/tpch_sf0_01' || '/orders/parquet/**/*.parquet');
create view part as from parquet_scan('s3://test-bucket-ceiveran/delta_testing/tpch_sf0_01' || '/part/parquet/**/*.parquet');
create view partsupp as from parquet_scan('s3://test-bucket-ceiveran/delta_testing/tpch_sf0_01' || '/partsupp/parquet/**/*.parquet');
create view region as from parquet_scan('s3://test-bucket-ceiveran/delta_testing/tpch_sf0_01' || '/region/parquet/**/*.parquet');
create view supplier as from parquet_scan('s3://test-bucket-ceiveran/delta_testing/tpch_sf0_01' || '/supplier/parquet/**/*.parquet');