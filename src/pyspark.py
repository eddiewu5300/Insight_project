# pyspark --conf spark.cassandra.connection.host="10.0.0.13" --master spark://ip-10-0-0-11:7077 --conf spark.executor.memoryOverhead=600 --executor-memory 6G --driver-memory 6G
# spark-submit --conf spark.cassandra.connection.host="10.0.0.13" --master spark://ip-10-0-0-11:7077 --conf spark.executor.memoryOverhead=600 --executor-memory 5G --total-executor-cores 12 --driver-memory 5G
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
# nltk.download("wordnet")
# nltk.download("stopwords")
warnings.filterwarnings("ignore")


def text_cleaning(sentence, stop):
    """
    Remove the punctuation & stop words
    sentence - str type
    return list of proccessed str
    """
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


def get_word2vec(sentence, model):
    """
    project each word to 50 dimention vector
    sentence - a list of string type
    return a list of vector
    """
    vec_ = []
    for idx in range(len(sentence)):
        tmp = sentence[idx]
        try:
            vec = model.wv[tmp]
        except:
            vec = np.repeat(0.0, 50)
        # vec_.append(np.round(vec, 5))
        vec_.append(vec)
    return vec_


def get_tfidf(sentence):
    """
    calculate the weight of each word in the sentence by pretrained tfidf
    sentence - a list of string type
    return a list of scaler
    """
    tfidf_ = []
    for idx in range(len(sentence)):
        w = sentence[idx]
        tfidf = 0.1 * math.log(wikiwords.N * wikiwords.freq(w.lower()) + 10)
        tfidf = float('%.2f' % tfidf)
        tfidf_.append(tfidf)
    return tfidf_


def sentence_embeded(sentence, model):
    """
    multipy word vectors with the weight(scaler)
    sentence - a list of string type
    return a vector
    """
    weight = get_tfidf(sentence)
    word_vector = get_word2vec(sentence, model)
    weighted_sentence = [x * y for x, y in zip(weight, word_vector)]
    embeded_sentence = sum(weighted_sentence)
    return (embeded_sentence)


def vote(sim, count, treshold):
    """
    to take the vote, if the majorty of the reviews are similar, it is a potential fake account
    sim - a list of
    """
    if len(sim) < 1:
        return False
    vote = [True if si > treshold else False for si in sim]
    if Counter(vote)[1] > count//2 + 1:
        return True
    else:
        return False


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
        result.append(float(sim))
    return result


def load_model(sc):
    now = datetime.datetime.now()
    print('loading model')
    model = KeyedVectors.load_word2vec_format(
        'glove.6B.50d.txt.word2vec', binary=False)
    print('model loading time')
    print(str(datetime.datetime.now()-now) + 'sec')
    model_broadcast = sc.broadcast(model)
    return model


def rdd2DF(rdd, sc):
    schema = StructType(
        [
            StructField("user_id", StringType(), True),
            StructField("count", IntegerType(), True),
            StructField("fake", BooleanType(), True),
            StructField("review", ArrayType(ArrayType(FloatType())), True),
            StructField("similarity", ArrayType(FloatType()), True)
        ])
    sqlContext = SQLContext(sc)
    users_df = sqlContext.createDataFrame(rdd, schema)
    users_df.show(3)
    return users_df


def main(data_path):
    sc = SparkContext()
    spark = SparkSession(sc)
    new_reviews = spark.read.parquet(data_path)
    model = load_model(sc)
    # Preload stop words
    stop = set(stopwords.words('english'))

    review_rdd = new_reviews.select('customer_id', 'review_body').rdd\
        .map(lambda x: (x[0], str(x[1])))\
        .map(lambda x: (x[0], text_cleaning(x[1], stop)))\
        .filter(lambda x: len(x[1]) > 0)\
        .map(lambda x: (x[0], ([sentence_embeded(x[1], model)], 1)))\
        .reduceByKey(lambda a, b: (a[0]+b[0],  a[1]+a[1]))

    # similarity: Row(id, list(sentence vectors), list(similarity(float)), count)
    # vote: Row(id, count, fake or not, list(sentence vectors), list(similarity) )
    fake_account_rdd = review_rdd\
        .map(lambda x: (x[0], [x.tolist() for x in x[1][0]],
                        calculate_similarity(x[1][0]), x[1][1]))\
        .map(lambda x: (x[0], x[3], vote(x[2], x[3], 0.8), x[1], x[2]))\
        .filter(lambda x: x[1] < 1000)

    print('#'*100)
    print('RDD ready')
    print('Transfering to DF')
    fake_account_df = rdd2DF(fake_account_rdd, sc)

    print('#'*100)
    print('writing data into Cassandra')
    fake_account_df.write.format("org.apache.spark.sql.cassandra")\
        .mode('append')\
        .options(table="users", keyspace="project")\
        .save()
    print('Data Stored in Cassandra')


main("s3a://amazondata/new_reviews/today/part-00006-495c48e6-96d6-4650-aa65-3c36a3516ddd.c000.snappy.parquet")
# write into spark


# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages \
#     com.datastax.spark:spark-cassandra-connector_2.11:2.3.2 \
#         --conf spark.cassandra.connection.host=10.0.0.13 pyspark-shell'


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
#     return resul
