"""InfluxDB input and output

Copyright Amiigo Inc.
"""


from common.influxdb.indb_io import query_indb, get_auth_indb
from common.influxdb.indb_pd import query_indb_df

auth = get_auth_indb('admin', 'password')
query_indb(auth, "CREATE DATABASE mydb")
query_indb(auth, "CREATE RETENTION POLICY short ON mydb DURATION 7d REPLICATION 1")
query_indb(auth, "CREATE USER dashesy WITH PASSWORD 'test_write'")
query_indb(auth, "CREATE USER boz WITH PASSWORD 'test_read'")
query_indb(auth, "GRANT ALL ON mydb TO dashesy")
query_indb(auth, "GRANT READ ON mydb TO boz")

query_indb_df(auth, 'SHOW DATABASES')
query_indb_df(auth, 'SHOW RETENTION POLICIES mydb')
query_indb_df(auth, 'SHOW MEASUREMENTS', database='_internal')
query_indb_df(auth, 'SELECT * from server', database='_internal')
