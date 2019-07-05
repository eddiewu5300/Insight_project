# pyspark --conf spark.cassandra.connection.host="10.0.0.13" --master spark://ip-10-0-0-11:7077 --conf spark.executor.memoryOverhead=600 --executor-memory 6G --driver-memory 6G
# spark-submit --conf spark.cassandra.connection.host="10.0.0.13" --master spark://ip-10-0-0-11:7077 --conf spark.executor.memoryOverhead=600 --executor-memory 5G --total-executor-cores 12 --driver-memory 5G
import sys
import os
import configparser
from pyspark.sql.types import *
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, concat, col, lit
from collections import Counter
import logging
from gensim.models.keyedvectors import KeyedVectors
from nltk.stem import WordNetLemmatizer
import warnings
from nltk.stem import PorterStemmer
from nltk.corpus import stopwords
import nltk
from sklearn.metrics.pairwise import cosine_similarity
import math
import wikiwords
# import config
import datetime
import sys
import os
import re
import time
import numpy as np
# from termcolor import colored
from itertools import combinations
from cassandra import cassandra_store
from config.config import *
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
    weighted_sentence = [np.round(x * y, 2)
                         for x, y in zip(weight, word_vector)]
    embeded_sentence = sum(weighted_sentence)
    return embeded_sentence


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
    """
    Load word2vec model
    """
    now = datetime.datetime.now()
    print('loading model')
    model = KeyedVectors.load_word2vec_format(
        'glove.6B.50d.txt.word2vec', binary=False)
    print('model loading time')
    print(str(datetime.datetime.now()-now) + 'sec')
    model_broadcast = sc.broadcast(model)
    return model


def main(path):
    df = spark.read.parquet(path + "product_category=Apparel/*.parquet")
    df = df.select('customer_id', 'review_body')
    now = datetime.datetime.now()
    model = load_model(sc)
    print('*'*100)
    print('processing data')

    stop = set(stopwords.words('english'))
    now = datetime.datetime.now()

    review_rdd = df.select('customer_id', 'review_body').rdd\
        .map(lambda x: (x[0], str(x[1])))\
        .map(lambda x: (x[0], text_cleaning(x[1], stop)))\
        .filter(lambda x: len(x[1]) > 0)\
        .map(lambda x: (x[0], ([sentence_embeded(x[1], model)], 1)))\
        .reduceByKey(lambda a, b: (a[0]+b[0],  a[1]+a[1]))
    review_rdd.take(1)
    print('*'*100)
    print('done')
    print(datetime.datetime.now()-now)


if __name__ == "__main__":
    sc = SparkContext()
    spark = SparkSession(sc)
    model = load_model(sc)
    main("s3a://amazondata/parquet/")
