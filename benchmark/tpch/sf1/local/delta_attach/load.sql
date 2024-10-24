ATTACH './data/generated/tpch_sf1/customer/delta_lake' as customer (TYPE delta);
ATTACH './data/generated/tpch_sf1/lineitem/delta_lake' as lineitem (TYPE delta);
ATTACH './data/generated/tpch_sf1/nation/delta_lake' as nation (TYPE delta);
ATTACH './data/generated/tpch_sf1/orders/delta_lake' as orders (TYPE delta);
ATTACH './data/generated/tpch_sf1/part/delta_lake' as part (TYPE delta);
ATTACH './data/generated/tpch_sf1/partsupp/delta_lake' as partsupp (TYPE delta);
ATTACH './data/generated/tpch_sf1/region/delta_lake' as region (TYPE delta);
ATTACH './data/generated/tpch_sf1/supplier/delta_lake' as supplier (TYPE delta);