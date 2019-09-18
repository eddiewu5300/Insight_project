from cassandra.cluster import Cluster
from pyspark.sql.types import *
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_set, collect_list, count, udf, concat, col, lit
from collections import Counter
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
from cassandra import cassandra_store
from config.cofig import *
import logging

logger = logging.getLogger(__name__)
logger.setLevel("INFO")
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
file_handler = logging.FileHandler('daily_job.log')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


def load_model(sc):
    """
    Load global Vec model
    """
    now = datetime.datetime.now()
    logger.info('loading model')
    model = KeyedVectors.load_word2vec_format(
        'glove.6B.50d.txt.word2vec', binary=False)
    logger.info('model loading time')
    logger.info(str(datetime.datetime.now()-now) + 'sec')
    model_broadcast = sc.broadcast(model)
    return model


def lemmatize(tokens):
    wordnet_lemmatizer = WordNetLemmatizer()
    stems = [wordnet_lemmatizer.lemmatize(token)
             for token in tokens if len(token) > 1]
    return stems


def filter_body(body):
    cleanr = re.compile('<.*?>')
    body = re.sub(cleanr, '', str(body))
    body = re.sub(r'[?|!|\'|"|#]', "", body)
    body = re.sub(r"[^\w\s]", "", body)
    body = re.sub(r'[.|,|)|(|\|/]', "", body)
    body = re.sub(r'\d+', r'', body)
    return str(body)


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
            logger.warning('{} - Failed to convert to wordvector'.format(tmp))
            vec = np.repeat(0.0, 50)
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
        try:
            tfidf = 0.1 * math.log(wikiwords.N *
                                   wikiwords.freq(w.lower()) + 10)
            tfidf = float('%.2f' % tfidf)
        except:
            logger.warning('{} - Failed to get to tfidf'.format(w.lower))
            tfidf = 0.0
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
    weighted_sentence = [x * y
                         for x, y in zip(weight, word_vector)]
    embeded_sentence = sum(weighted_sentence)
    embeded_sentence = np.round(embeded_sentence, 2)
    embeded_sentence = embeded_sentence.tolist()
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


def query_from_cassandra(id, session, table, keyspace='project'):
    """
    id - a user id, primary key in cassandra
    return the value from key
    """
    try:
        row = session.execute(
            "SELECT * FROM project.{} WHERE user_id ='{}';".format(table, id)
        )[0]
    except:
        return 'user not in database', None, None
    reviews = row[3]
    count = row[1]
    similarity = row[4]
    if similarity is None:
        similarity = []
    return count, reviews, similarity


def update_data(id, new_reviews, new_count, session, table):
    count, reviews, similarity = query_from_cassandra(id, session, table)
    if count == 'user not in database':
        new_reviews = [x.tolist() for x in new_reviews]
        session.execute(
            """
            INSERT INTO {} (user_id, count, fake, review, similarity) VALUES('{}',{},{},{},{});
            """.format(table, id, new_count, False, new_reviews, [])
        )
    else:
        for new_review in new_reviews:
            for review in reviews:
                sim = cosine_similarity(np.array(review).reshape(
                    1, -1), new_review.reshape(1, -1))
                similarity.append(float(sim))
            reviews.append(new_review.tolist())
        count += new_count
        fake = vote(similarity, count, 0.9)
        session.execute(
            """
            UPDATE {} SET count={}, fake={}, review={}, similarity={} WHERE user_id='{}';
            """.format(table, count, fake, reviews, similarity, id)
        )


def main(data_path):
    sc = SparkContext()
    spark = SparkSession(sc)
    model = load_model(sc)
    # Preload stop words
    stop = set(stopwords.words('english'))
    logger.info("Spark jobs start")

    try:
        cass = cassandra_store.PythonCassandraExample(
            host=config['cassandra_ip'], keyspace=config['cassandra_keyspace'])
        cass.createsession()
    except:
        logger.error("Cannot connect to Cassandra")

    for cat in config['categories']:
        logger.info('Processing {}'.format(cat))
        path = data_path + 'new_reviews' + str(cat) + '/*.parquet'
        new_reviews = spark.read.parquet(path)

        clean_test = udf(lambda body: filter_body(body), StringType())
        cleaned_reviews = new_reviews.withColumn(
            "cleaned_text", clean_test("review_body"))

        tokenizer = Tokenizer(inputCol="cleaned_body",
                              outputCol="tokenized")
        tokenized = tokenizer.transform(cleaned_reviews)

        stop_words_remover = StopWordsRemover(
            inputCol="tokenized", outputCol="stop_words_removed")
        stop_word_removed = stop_words_remover.transform(tokenized)

        lemmatizer = udf(lambda tokens: lemmatize(
            tokens), ArrayType(StringType()))
        lemmatized = stop_word_removed.withColumn(
            "stemmed", lemmatizer("stop_words_removed"))

        sen_embeder = udf(lambda ls: sentence_embeded(
            ls, model), ArrayType(FloatType()))
        sen_embeded = lemmatized.withColumn("sen_vec", sen_embeder("stemmed"))

        df = sen_embeded.select("customer_id", "sen_vec")

        df_reduce = df.groupby("customer_id")\
            .agg(
                collect_list("sen_vec").alias("reviews"),
                count(lit(1)).alias("count")
        )
        # output Row((id, (list(sentence vectors), count)))

        collection = df_reduce.collect()

        cat = cat.lower().replace('&', '')
        for user_review in collection:
            id = user_review[0]
            reviews = user_review[1][0]
            count = user_review[1][1]
            update_data(id, reviews, count, cass, cat)
        logger.info('{} table update completed'.format(cat))
    logger.info('Database update completed')
    #review_rdd.map(lambda x: update_data(x[0], x[1][0], x[1][1], session))


if __name__ == "__main__":
    main("s3a://amazondata/new_reviews/")
