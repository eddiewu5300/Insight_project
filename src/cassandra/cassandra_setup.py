from datastore import cassandra_store
from config.config import *

cass = cassandra_store.PythonCassandra(
    host=config['cassandra_host'], keyspace=config['cassandra_keyspace'])
cass.createsession()
cass.create_tables()
