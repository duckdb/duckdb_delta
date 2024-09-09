CREATE SECRET IF NOT EXISTS s1 (type s3, provider credential_chain);

SET VARIABLE parquet_path = 's3://test-bucket-ceiveran/delta_benchmarking/tpcds_sf1_pyspark';

create view call_center as from parquet_scan(getvariable('parquet_path') || '/call_center/parquet/**/*.parquet');
create view catalog_page as from parquet_scan(getvariable('parquet_path') || '/catalog_page/parquet/**/*.parquet');
create view catalog_returns as from parquet_scan(getvariable('parquet_path') || '/catalog_returns/parquet/**/*.parquet');
create view catalog_sales as from parquet_scan(getvariable('parquet_path') || '/catalog_sales/parquet/**/*.parquet');
create view customer as from parquet_scan(getvariable('parquet_path') || '/customer/parquet/**/*.parquet');
create view customer_demographics as from parquet_scan(getvariable('parquet_path') || '/customer_demographics/parquet/**/*.parquet');
create view customer_address as from parquet_scan(getvariable('parquet_path') || '/customer_address/parquet/**/*.parquet');
create view date_dim as from parquet_scan(getvariable('parquet_path') || '/date_dim/parquet/**/*.parquet');
create view household_demographics as from parquet_scan(getvariable('parquet_path') || '/household_demographics/parquet/**/*.parquet');
create view inventory as from parquet_scan(getvariable('parquet_path') || '/inventory/parquet/**/*.parquet');
create view income_band as from parquet_scan(getvariable('parquet_path') || '/income_band/parquet/**/*.parquet');
create view item as from parquet_scan(getvariable('parquet_path') || '/item/parquet/**/*.parquet');
create view promotion as from parquet_scan(getvariable('parquet_path') || '/promotion/parquet/**/*.parquet');
create view reason as from parquet_scan(getvariable('parquet_path') || '/reason/parquet/**/*.parquet');
create view ship_mode as from parquet_scan(getvariable('parquet_path') || '/ship_mode/parquet/**/*.parquet');
create view store as from parquet_scan(getvariable('parquet_path') || '/store/parquet/**/*.parquet');
create view store_returns as from parquet_scan(getvariable('parquet_path') || '/store_returns/parquet/**/*.parquet');
create view store_sales as from parquet_scan(getvariable('parquet_path') || '/store_sales/parquet/**/*.parquet');
create view time_dim as from parquet_scan(getvariable('parquet_path') || '/time_dim/parquet/**/*.parquet');
create view warehouse as from parquet_scan(getvariable('parquet_path') || '/warehouse/parquet/**/*.parquet');
create view web_page as from parquet_scan(getvariable('parquet_path') || '/web_page/parquet/**/*.parquet');
create view web_returns as from parquet_scan(getvariable('parquet_path') || '/web_returns/parquet/**/*.parquet');
create view web_sales as from parquet_scan(getvariable('parquet_path') || '/web_sales/parquet/**/*.parquet');
create view web_site as from parquet_scan(getvariable('parquet_path') || '/web_site/parquet/**/*.parquet');