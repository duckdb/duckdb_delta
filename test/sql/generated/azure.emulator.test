# name: test/sql/generated/azure.emulator
# description: test delta scan on azure emulator data using secret
# group: [delta_generated]

require parquet

require httpfs

require azure

require delta

require-env GENERATED_AZURE_DATA_AVAILABLE

statement ok
CREATE SECRET azure_1 (TYPE AZURE, CONNECTION_STRING 'AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProtocol=http;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1')

# Run modified tpch q06 against the remote data
query I rowsort q1
SELECT
    *
FROM
    delta_scan('az://test-bucket-ceiveran/delta_testing/lineitem_sf0_01/delta_lake/')
LIMIT 100
----