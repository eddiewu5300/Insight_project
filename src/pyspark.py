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
import config
from pyspark import SparkContext
from pyspark.sql import SparkSession
import datetime
from pyspark.sql.types import ArrayType, StringType, FloatType, IntegerType
from pyspark.sql.functions import udf, concat, col, lit
import sys
import os
import re
import time
from termcolor import colored
# nltk.download("wordnet")
# nltk.download("stopwords")
warnings.filterwarnings("ignore")

sc = SparkContext()
spark = SparkSession(sc)
df = spark.read.parquet('./test_data.parquet')
print("*"*50)
print('start')


now = datetime.datetime.now()
print('loading model')
model = KeyedVectors.load_word2vec_format(
    'GoogleNews-vectors-negative300.bin', binary=True)
print('model loading time')
print(str(datetime.datetime.now()-now) + 'sec')

stop = set(stopwords.words('english'))


def text_cleaning(sentence, stop=stop):
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
    vec_ = []
    for idx in range(len(sentence)):
        tmp = sentence[idx]
        try:
            vec = model.wv[tmp]
        except:
            vec = np.repeat(0.0, 300)
        vec_.append(vec)
    return vec_


def get_tf(sentence):
    tf_ = []
    for idx in range(len(sentence)):
        tf = [0.1 * math.log(wikiwords.N * wikiwords.freq(w.lower()) + 10)
              for w in sentence[idx]]
        tf = [float('%.2f' % v) for v in tf]
    tf_.append(tf)
    return tf


def sentence_embeded(sentence):
    sentence = text_cleaning(sentence)
    weight = get_tf(sentence)
    word_vector = get_word2vec(sentence)
    weighted_sentence = [x * y for x, y in zip(weight, word_vector)]
    embeded_sentence = sum(weighted_sentence)
    return (embeded_sentence)


# if(config.LOG_DEBUG):
#    print(colored("[PROCESSING]: Cleaning question body...", "green"))
clean_text = udf(lambda text: text_cleaning(text), StringType())
text_cleaned = df.withColumn("cleaned_text", clean_text("review_body"))

len_text = udf(lambda ls: len(ls), StringType())
proccessed_df = text_cleaned.withColumn("text_length", len_text("review_body"))

# sen_embeded = udf(lambda ls: sentence_embeded(ls), StringType())
# embeded_df  = proccessed_df.withColumn("sen_vec", sen_embeded("cleaned_text"))


tf_try = udf(lambda ls: get_tf(ls), StringType())
tf_df = proccessed_df.withColumn("tf_vec", tf_try("cleaned_text"))

word2vec_try = udf(lambda ls: get_word2vec(ls), ArrayType(FloatType()))
word2vec_df = proccessed_df.withColumn(
    "word2vec", word2vec_try("cleaned_text"))


df2 = proccessed_df.select('customer_id', 'text_length', 'cleaned_text')


def reduce_by_user(a, b):
    result = [a, b]
    return result


df3 = df2.rdd.map(lambda x: (x[0], (x[1], x[2])))
df3.reduceByKey(reduce_by_user).take(1)
