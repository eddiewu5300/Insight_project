"""
Python  by Techfossguru
Copyright (C) 2017  Satish Prasad
Adapted by Eddie
This file is used by setup.py to create necessary cassandra tables
"""
import logging
from cassandra.cluster import Cluster


class PythonCassandraExample:

    def __init__(self, host, keyspace):
        self.cluster = None
        self.session = None
        self.keyspace = keyspace
        self.host = host

    def __del__(self):
        self.cluster.shutdown()

    def createsession(self):
        self.cluster = Cluster(self.host)
        self.session = self.cluster.connect(self.keyspace)
        print("Session Created!")

    def getsession(self):
        return self.session

    # How about Adding some log info to see what went wrong
    # def setlogger(self):
    #     log = logging.getLogger()
    #     log.setLevel('INFO')
    #     handler = logging.StreamHandler()
    #     handler.setFormatter(logging.Formatter(
    #         "%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
    #     log.addHandler(handler)
    #     self.log = log

    # Create Keyspace based on Given Name
    def createkeyspace(self, keyspace, drop=False):
        """
        :param keyspace:  The Name of Keyspace to be created
        :return:
        """
        # Before we create new lets check if exiting keyspace; we will drop that and create new
        rows = self.session.execute(
            "SELECT keyspace_name FROM system_schema.keyspaces")
        if keyspace in [row[0] for row in rows]:
            if drop:
                self.log.info("dropping existing keyspace...")
                self.session.execute("DROP KEYSPACE " + keyspace)
            else:
                self.log.info("existing keyspace, not doing anything...")
                return

        print("creating keyspace...")
        self.session.execute("""
                CREATE KEYSPACE %s
                WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
                """ % keyspace)

        print("setting keyspace...")
        self.session.set_keyspace(keyspace)

    def create_tables(self, table):
        c_sql = """
                CREATE TABLE IF NOT EXISTS {}  (
                user_id text PRIMARY KEY,
                count int,
                fake boolean,
                review list<frozen<list<float>>>,
                similarity list<float>);
                """.format(str(table))
        self.session.execute(c_sql)
        print(str(table)+" Table Created !!!")
