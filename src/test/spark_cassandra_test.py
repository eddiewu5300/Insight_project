# spark-submit --conf spark.cassandra.connection.host="10.0.0.13" pyspark_cassandra_connections.py
# Configuratins related to Cassandra connector & Cluster
from pyspark.sql import SQLContext
from pyspark.sql.functions import lit
from pyspark import SparkContext
import os


os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.11:2.3.2 --conf spark.cassandra.connection.host=10.0.0.13 pyspark-shell'
# sc = SparkContext("local", "spark_job")

# Creating PySpark SQL Context
sqlContext = SQLContext(sc)

# Loads and returns data frame for a table including key space given


def load_and_get_table_df(keys_space_name, table_name):
    table_df = sqlContext.read\
        .format("org.apache.spark.sql.cassandra")\
        .options(table=table_name, keyspace=keys_space_name)\
        .load()
    return table_df


# Loading movies & ratings table data frames
seconds = load_and_get_table_df("test", "test")

# seconds.show()

minutes = seconds.agg({'subs': 'avg'})

minutes = minutes.withColumn('avg_name', lit('first average'))\
    .withColumnRenamed('avg(subs)', 'avg_value')\
    .select('avg_name', 'avg_value')

minutes.show()
minutes.write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="avgs", keyspace="test")\
    .save()
