# pyspark --conf spark.cassandra.connection.host="10.0.0.13" --master spark://ip-10-0-0-11:7077 --conf spark.executor.memoryOverhead=600 --executor-memory 6G --driver-memory 6G
# spark-submit --conf spark.cassandra.connection.host="10.0.0.13" --master spark://ip-10-0-0-11:7077 --conf spark.executor.memoryOverhead=600 --executor-memory 6G --driver-memory 6G spark_job.py
from pyspark.sql.types import *
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, concat, col, lit
from collections import Counter
import logging
from gensim.models.keyedvectors import KeyedVectors
from pyspark.ml.feature import StopWordsRemover
from nltk.stem import WordNetLemmatizer
import warnings
from nltk.stem import PorterStemmer
from nltk.corpus import stopwords
import nltk
from sklearn.metrics.pairwise import cosine_similarity
import math
import wikiwords
#import config
import datetime
import sys
import os
import re
import time
import numpy as np
# from termcolor import colored
from itertools import combinations
nltk.download("wordnet")
nltk.download("stopwords")
warnings.filterwarnings("ignore")

# Start
sc = SparkContext()
spark = SparkSession(sc)
df = spark.read.parquet(
    's3a://amazondata/parquet/product_category=Apparel/part-00000-495c48e6-96d6-4650-aa65-3c36a3516ddd.c000.snappy.parquet')
print("*"*50)
print('start')

# Load word2vec model
now = datetime.datetime.now()
print('loading model')
model = KeyedVectors.load_word2vec_format(
    'glove.6B.50d.txt.word2vec', binary=False)
print('model loading time')
print(str(datetime.datetime.now()-now) + 'sec')
model_broadcast = sc.broadcast(model)

# Preload stop words
stop = set(stopwords.words('english'))


def text_cleaning(sentence, stop=stop):
    """
    Remove the punctuation & stop words
    sentence - str type 
    return list of proccessed str
    """
    # if len(sentence) < 1:
    #     return False
    lemmatizer = WordNetLemmatizer()
    sentence = sentence.lower()
    cleanr = re.compile('<.*?>')
    sentence = re.sub(cleanr, ' ', sentence)
    sentence = re.sub(r'[?|!|\'|"|#]', r'', sentence)
    sentence = re.sub(r'[.|,|)|(|\|/]', r' ', sentence)
    sentence = re.sub(r'\d+', r' ', sentence)
    words = [lemmatizer.lemmatize(word) for word in sentence.split(
    ) if word not in stop and len(word) > 1]
    return words


def get_word2vec(sentence):
    """
    project each word to 50 dimention vector
    sentence - a list of string type 
    return a list of vector 
    """
    # if len(sentence) < 1:
    #     return False
    vec_ = []
    for idx in range(len(sentence)):
        tmp = sentence[idx]
        try:
            vec = model.wv[tmp]
        except:
            vec = np.repeat(0.0, 50)
        vec_.append(vec)
    return vec_


def get_tf(sentence):
    """
    calculate the weight of each word in the sentence by pretrained tfidf
    sentence - a list of string type 
    return a list of scaler 
    """
    tf_ = []
    for idx in range(len(sentence)):
        w = sentence[idx]
        tf = 0.1 * math.log(wikiwords.N * wikiwords.freq(w.lower()) + 10)
        tf = float('%.2f' % tf)
        tf_.append(tf)
    return tf_


def sentence_embeded(sentence):
    """
    multipy word vectors with the weight(scaler)
    sentence - a list of string type 
    return a vector
    """
    weight = get_tf(sentence)
    word_vector = get_word2vec(sentence)
    weighted_sentence = [x * y for x, y in zip(weight, word_vector)]
    embeded_sentence = sum(weighted_sentence)
    return (embeded_sentence)


test = df.select('customer_id', 'review_body')

print("*"*50)
print('convert to string')
test2 = test.rdd.map(lambda x: (x[0], str(x[1])))

test3 = test2.map(lambda x: (x[0], text_cleaning(x[1])))

test4 = test3.filter(lambda x: len(x[1]) > 0)
test5 = test4.map(lambda x: (x[0], ([sentence_embeded(x[1])], [len(x[1])], 1)))

# reduceby userid
print("*"*50)
print('reduce by user')
test6 = test5.reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1], a[2]+a[2]))


def calculate_similarity(ls):
    """
    ls - a list of vec
    each vec is a numpy array
    return a list of similarity of pair of reviews
    """
    result = []
    for i in combinations(ls, 2):
        sim = (cosine_similarity(i[0].reshape(1, -1), i[1].reshape(1, -1)))
        sim = round(sim[0][0], 2)
        result.append(sim)
    return result


print("*"*50)
print('similarity calculation')
test7 = test6.map(lambda x: (
    x[0], [x.tolist() for x in x[1][0]], calculate_similarity(x[1][0]), x[1][2]))


def vote(sim, count):
    if len(sim) < 1:
        return True
    vote = [True if si > 0.6 else False for si in sim]
    if Counter(vote)[1] < count//2 + 1:
        return True
    else:
        return False


print("*"*50)
print('detect fake account')
test8 = test7.map(lambda x: (
    (x[0]), int(x[3]), vote(x[2], x[3]), x[1]))
#rdd = test8.collect()


# schema = StructType(
#     [
#         StructField("user_id", StringType(), True),
#         StructField("count", IntegerType(), True),
#         StructField("fake", BooleanType(), True),
#         StructField("reveiw", ArrayType(
#             StructType([
#                 StructField("vector", ArrayType(FloatType()), True)
#             ])
#         ), True)
#     ])

# StructType(List(StructField(user_id,StringType,true)\
# ,StructField(count,IntegerType,true),StructField(fake,BooleanType,true)\
# ,StructField(reveiw,ArrayType(ArrayType(FloatType,true),true),true)))


schema = StructType(
    [
        StructField("user_id", StringType(), True),
        StructField("count", IntegerType(), True),
        StructField("fake", BooleanType(), True),
        StructField("review", ArrayType(ArrayType(FloatType())), True)
    ])

sqlContext = SQLContext(sc)
df2 = sqlContext.createDataFrame(test8, schema)
df2.show(3)
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages \
    com.datastax.spark:spark-cassandra-connector_2.11:2.3.2 \
        --conf spark.cassandra.connection.host=10.0.0.13 pyspark-shell'
df2.write.format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="user_reviews", keyspace="project")\
    .save()


# if(config.LOG_DEBUG):
#    print(colored("[PROCESSING]: Cleaning question body...", "green"))
# clean_text = udf(lambda text: text_cleaning(text), StringType())
# text_cleaned = df.withColumn("cleaned_text", clean_text("review_body"))

# len_text = udf(lambda ls: len(ls), IntegerType())
# proccessed_df = text_cleaned.withColumn(
#     "text_length", len_text("cleaned_text"))

# sen_embeded = udf(lambda ls: sentence_embeded(ls), StringType())
# embeded_df  = proccessed_df.withColumn("sen_vec", sen_embeded("cleaned_text"))


# tf_try = udf(lambda ls: get_tf(ls), StringType())
# tf_df = proccessed_df.withColumn("tf_vec", tf_try("cleaned_text"))

# word2vec_try = udf(lambda ls: get_word2vec(ls), ArrayType(FloatType()))
# word2vec_df = proccessed_df.withColumn(
#     "word2vec", word2vec_try("cleaned_text"))


# df2 = proccessed_df.select('customer_id', 'text_length', 'cleaned_text')


# def reduce_by_user(a, b):
#     result = [a, b]
#     return result


# df3 = df2.rdd.map(lambda x: (x[0], (x[1], x[2])))
# df3.reduceByKey(reduce_by_user).take(1)
