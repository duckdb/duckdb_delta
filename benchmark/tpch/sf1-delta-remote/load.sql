create view customer as from delta_scan('s3://test-bucket-ceiveran/delta_testing/tpch_sf0_01' || '/customer/delta_lake');
create view lineitem as from delta_scan('s3://test-bucket-ceiveran/delta_testing/tpch_sf0_01' ||  '/lineitem/delta_lake');
create view nation as from delta_scan('s3://test-bucket-ceiveran/delta_testing/tpch_sf0_01' || '/nation/delta_lake');
create view orders as from delta_scan('s3://test-bucket-ceiveran/delta_testing/tpch_sf0_01' || '/orders/delta_lake');
create view part as from delta_scan('s3://test-bucket-ceiveran/delta_testing/tpch_sf0_01' || '/part/delta_lake');
create view partsupp as from delta_scan('s3://test-bucket-ceiveran/delta_testing/tpch_sf0_01' || '/partsupp/delta_lake');
create view region as from delta_scan('s3://test-bucket-ceiveran/delta_testing/tpch_sf0_01' || '/region/delta_lake');
create view supplier as from delta_scan('s3://test-bucket-ceiveran/delta_testing/tpch_sf0_01' || '/supplier/delta_lake');