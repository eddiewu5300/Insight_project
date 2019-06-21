from datastore import cassandra_store
from config.config import *

cass = cassandra_store.PythonCassandraExample(
    host="10.0.0.13", keyspace="project")
cass.createsession()
# cass.setlogger()
cass.create_tables()
