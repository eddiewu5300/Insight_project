from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import collect_set, collect_list, count, udf, concat, col, lit
from pyspark.ml.feature import Tokenizer
from pyspark.ml.feature import StopWordsRemover
from pyspark.sql.types import *
from collections import Counter
from gensim.models.keyedvectors import KeyedVectors
from nltk.stem import WordNetLemmatizer
import wikiwords
# nltk.download("wordnet")
from sklearn.metrics.pairwise import cosine_similarity
import math
import datetime
import re
import time
import numpy as np
# from termcolor import colored
from itertools import combinations
from cassandra import cassandra_store
from config.config import *
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.info)
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
file_handler = logging.FileHandler('spark_job.log')
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


def calculate_similarity(ls):
    """
    ls - a list of vec
    return a list of similarity of pair of reviews
    """
    result = []
    for i in combinations(ls, 2):
        sim = (cosine_similarity(i[0].reshape(1, -1), i[1].reshape(1, -1)))
        sim = round(sim[0][0], 2)
        result.append(float(sim))
    return result


def main(data_path):
    sc = SparkContext()
    spark = SparkSession(sc)
    model = load_model(sc)
    logger.info("Spark jobs start")

    try:
        cass = cassandra_store.PythonCassandraExample(
            host=config['cassandra_ip'], keyspace=config['cassandra_keyspace'])
        cass.createsession()
    except:
        logger.error("Cannot connect to Cassandra")

    categories = config['contegories']
    for cat in categories:
        logger.info('Processing {}'.format(cat))
        path = data_path + 'product_category=' + str(cat) + '/*.parquet'
        reviews = spark.read.parquet(path)

        ##########################################################
        # save the raw text table
        df2 = reviews.selectExpr('customer_id', 'review_id', 'product_id',
                                 'product_title', 'star_rating', 'review_body', "'{}' as category".format(cat))
        df3 = df2.dropDuplicates()

        lower_case = udf(lambda string: string.lower(), StringType())
        df4 = df3.withColumn("product_title", lower_case('product_title'))

        logger.info('creating raw review table')
        cat1 = cat.lower().replace('&', '') + '_review'
        cass.create_text_tables(cat1)
        logger.info('creating index')
        cass.create_text_index(cat1)

        df4.write.format("org.apache.spark.sql.cassandra")\
            .mode('append')\
            .options(table=cat1, keyspace="project")\
            .save()
        logger.info('Review Data Stored in Cassandra')

        ##########################################################
        # process fake account information

        # text processing
        clean_test = udf(lambda body: filter_body(body), StringType())
        cleaned_reviews = reviews.withColumn(
            "cleaned_text", clean_test("review_body"))

        # tokenization
        tokenizer = Tokenizer(inputCol="cleaned_body",
                              outputCol="tokenized")
        tokenized = tokenizer.transform(cleaned_reviews)

        # remove stop words
        stop_words_remover = StopWordsRemover(
            inputCol="tokenized", outputCol="stop_words_removed")
        stop_word_removed = stop_words_remover.transform(tokenized)

        # lemmatization
        lemmatizer = udf(lambda tokens: lemmatize(
            tokens), ArrayType(StringType()))
        lemmatized = stop_word_removed.withColumn(
            "stemmed", lemmatizer("stop_words_removed"))

        # sentence embedding(word embedding * tfidf)
        sen_embeder = udf(lambda ls: sentence_embeded(
            ls, model), ArrayType(FloatType()))
        sen_embeded = lemmatized.withColumn("sen_vec", sen_embeder("stemmed"))

        df = sen_embeded.select("customer_id", "sen_vec")

        # reduce by user_id
        df_reduce = df.groupby("customer_id")\
            .agg(
                collect_list("sen_vec").alias("reviews"),
            count(lit(1)).alias("count")
        )

        # similarity: Row(id, list(sentence vectors), list(similarity(float)), count)
        similarity_cal = udf(lambda ls: calculate_similarity(
            ls), ArrayType(FloatType()))
        sim_df = df_reduce.withColumn("similarity", similarity_cal("reviews"))

        # vote: Row(id, count, fake or not, list(sentence vectors), list(similarity) )
        voter = udf(lambda ls: vote(
            ls, lit("count"), treshold=0.9), BooleanType())
        vote_df = sim_df.withColumn("fake", voter("similartity"))

        logger.info('writing data into Cassandra')
        cat = cat.lower().replace('&', '')
        vote_df.write.format("org.apache.spark.sql.cassandra")\
            .mode('append')\
            .options(table=cat, keyspace="project")\
            .save()
        logger.info('Data Stored in Cassandra')


if __name__ == "__main__":
    main("s3a://amazondata/parquet/")
