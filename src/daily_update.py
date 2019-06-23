from cassandra.cluster import Cluster
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
import datetime
import sys
import os
import re
import time
import numpy as np
from itertools import combinations


def load_model(sc):
    now = datetime.datetime.now()
    print('loading model')
    model = KeyedVectors.load_word2vec_format(
        'glove.6B.50d.txt.word2vec', binary=False)
    print('model loading time')
    print(str(datetime.datetime.now()-now) + 'sec')
    model_broadcast = sc.broadcast(model)
    return model_broadcast


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


def query_from_cassandra(id, session, keyspace='project', table='users'):
    """
    id - a user id, primary key in cassandra
    return the value from key
    """
    try:
        row = session.execute(
            "SELECT * FROM project.users WHERE user_id ='{}';".format(id)
        )[0]
    except:
        return 'user not in database', None, None
    reviews = row[3]
    count = row[1]
    similarity = row[4]
    if similarity is None:
        similarity = []
    return count, reviews, similarity


def update_data(id, new_reviews, new_count, session, table='users'):
    count, reviews, similarity = query_from_cassandra(id, session)
    print('query done:')
    if count == 'user not in database':
        new_reviews = [x.tolist() for x in new_reviews]
        session.execute(
            "INSERT INTO {} (user_id, count, fake, review, similarity) VALUES('{}',{},{},{},{});"
            .format(table, id, new_count, False, new_reviews, [])
        )
        print('new data')
    else:
        for new_review in new_reviews:
            for review in reviews:
                sim = cosine_similarity(np.array(review), new_review)
                similarity.append(float(sim))
            reviews.append(new_review.tolist())
        count += new_count
        fake = vote(similarity, count, 0.8)
        session.execute(
            "UPDATE {} SET count={}, fake={}, review={}, similarity={} WHERE user_id='{}';"
            .format(table, count, fake, reviews, similarity, id)
        )
        print('update')


def main(data_path):
    sc = SparkContext()
    spark = SparkSession(sc)
    new_reviews = spark.read.parquet(data_path)
    model = load_model(sc)
    # Preload stop words
    stop = set(stopwords.words('english'))

    # output Row((id, (list(sentence vectors), count)))
    review_rdd = new_reviews.select('customer_id', 'review_body').rdd\
        .map(lambda x: (x[0], str(x[1])))\
        .map(lambda x: (x[0], text_cleaning(x[1], stop)))\
        .filter(lambda x: len(x[1]) > 0)\
        .map(lambda x: (x[0], ([sentence_embeded(x[1], model)], 1)))\
        .reduceByKey(lambda a, b: (a[0]+b[0],  a[1]+a[1]))

    collection = review_rdd.collect()
    cluster = Cluster(['10.0.0.13'])
    session = cluster.connect('project')

    for user_review in collection:
        id = user_review[0]
        reviews = user_review[1][0]
        count = user_review[1][1]
        print(id, reviews, count)
        update_data(id, reviews, count, session)

    #review_rdd.map(lambda x: update_data(x[0], x[1][0], x[1][1], session))

    print('Data Base update completed')


main("s3a://amazondata/new_reviews/2019/part-00000-495c48e6-96d6-4650-aa65-3c36a3516ddd.c000.snappy.parquet")
