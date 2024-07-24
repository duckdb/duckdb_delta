create view customer as from delta_scan('./data/generated/tpch_sf1/customer/delta_lake');
create view lineitem as from delta_scan('./data/generated/tpch_sf1/lineitem/delta_lake');
create view nation as from delta_scan('./data/generated/tpch_sf1/nation/delta_lake');
create view orders as from delta_scan('./data/generated/tpch_sf1/orders/delta_lake');
create view part as from delta_scan('./data/generated/tpch_sf1/part/delta_lake');
create view partsupp as from delta_scan('./data/generated/tpch_sf1/partsupp/delta_lake');
create view region as from delta_scan('./data/generated/tpch_sf1/region/delta_lake');
create view supplier as from delta_scan('./data/generated/tpch_sf1/supplier/delta_lake');