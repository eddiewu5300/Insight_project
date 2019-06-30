

from pyspark.sql.types import *
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, concat, col, lit
from config.config import *


def pull_data(data_path):
    """
    pull data from s3 to cassandra
    """
    for cat in config['categories']:
        path = data_path + 'product_category=' + str(cat) + '/*.parquet'
        df = spark.read.parquet(path)

        cat = cat.lower().replace('&', '')
        df2 = df.selectExpr('customer_id', 'review_id', 'product_id',
                            'product_title', 'star_rating', 'review_body', "'{}' as category".format(cat))
        df3 = df2.dropDuplicates()

        lower_case = udf(lambda string: string.lower(), StringType())
        df4 = df3.withColumn("product_title", lower_case('product_title'))

        df4.write.format("jdbc").options(url='jdbc:postgresql://' + config['postgres_host']+'/project', dbtable=cat, user='postgres',
                                         password=config['postgres_password'], stringtype="unspecified").mode('append').save()


if __name__ == "__main__":
    sc = SparkContext()
    spark = SparkSession(sc).builder \
        .appName("postgres") \
        .getOrCreate()
    data_path = "s3a://amazondata/parquet/"
    print('starting pull data to postgres')
    pull_data(data_path)
