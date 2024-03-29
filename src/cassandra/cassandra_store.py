"""
Python  by Techfossguru
Copyright (C) 2017  Satish Prasad
Adapted by Eddie
This file is used by setup.py to create necessary cassandra tables
"""
import logging
from cassandra.cluster import Cluster
from config.config import *

logger = logging.getLogger(__name__)
logger.setLevel("INFO")
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
file_handler = logging.FileHandler('cassandra.log')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


class PythonCassandra:

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
        logger.info("Session Created!")

    def getsession(self):
        return self.session

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
                logger.info("dropping existing keyspace...")
                self.session.execute("DROP KEYSPACE " + keyspace)
            else:
                logger.info("existing keyspace, not doing anything...")
                return

        logger.info("creating keyspace...")
        self.session.execute("""
                CREATE KEYSPACE %s
                WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2' }
                """ % keyspace)

        logger.info("setting keyspace...")
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
        logger.info("{} Table Created !!!".format(table))

    def create_text_tables(self, table):
        c_sql = """
                CREATE TABLE project.{} (
                customer_id text PRIMARY KEY,
                product_id text,
                product_title text,
                review_body text,
                review_id text,
                star_rating int
            );
            """.format(str(table))
        self.session.execute(c_sql)
        logger.info("{} Text Table Created !!!".format(table))

    def create_text_index(self, table):
        c_sql = """
        CREATE CUSTOM INDEX query ON project.tmp (product_title) 
        USING 'org.apache.cassandra.index.sasi.SASIIndex' 
        WITH OPTIONS = 
        {'mode': 'CONTAINS', 
        'analyzer_class': 'org.apache.cassandra.index.sasi.analyzer.NonTokenizingAnalyzer', 
        'case_sensitive': 'false'};
        """.format(str(table))
        self.session.execute(c_sql)
        logger.info("{} Text Index Created !!!".format(table))
