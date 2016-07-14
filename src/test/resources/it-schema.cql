
CREATE KEYSPACE cassandra_migration_test
 WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
