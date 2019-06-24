from datastore import cassandra_store

cass = cassandra_store.PythonCassandraExample(
    host=["10.0.0.13"], keyspace="project")
cass.createsession()
# cass.setlogger()
cass.create_tables()
