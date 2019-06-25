# import psycopg2

# connection = psycopg2.connect(
#     host='127.0.0.1', database='project', user='postgres')

# cursor = connection.cursor()

# cursor.execute('''INSERT INTO review(user_id, product_id, product_name, review, rating ) \
#     VALUES('123', 234,'coffee','THIS coffee is sucks',3)''')


from pyspark.sql.types import *
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, concat, col, lit
categories = ['Apparel', 'Automotive', 'Baby', 'Beauty', 'Books', 'Camera', 'Digital_Ebook_Purchase', 'Electronics', 'Furniture', 'Health_&_Personal_Care', 'Home', 'Kitchen',
              'Lawn_and_Garden', 'Mobile_Apps', 'Music', 'Office_Products', 'Outdoors', 'PC', 'Pet_Products', 'Shoes', 'Sports', 'Toys', 'Video_DVD', 'Video_Games', 'Wireless']
sc = SparkContext()
spark = SparkSession.builder \
    .appName("postgres") \
    .config("spark.executor.memory", "2gb") \
    .getOrCreate()

df = spark.read.parquet(
    's3a://amazondata/parquet/product_category=Apparel/*.parquet')
df2 = df.selectExpr('customer_id', 'review_id', 'product_id',
                    'product_title', 'star_rating', 'review_body', "'Apparel' as category")
lower_case = udf(lambda string: string.lower(), StringType())
df3 = df2.withColumn("lower_title", lower_case('product_title'))


df3.write.format("jdbc").options(url='jdbc:postgresql://ec2-3-84-67-246.compute-1.amazonaws.com/postgres', dbtable='project.reviews', user='postgres',
                                 password='123', stringtype="unspecified").mode('append').save()
