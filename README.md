# FakeOUT

## Table of Contents
1. [Introduction](README.md#Introduction)
2. [Pipeline](README.md#Pipeline)
3. [Requirements](README.md#Requirements)
4. [Environment Setup](README.md#Environment%20Setup)
5. [Run Instructions](README.md#Run%20Instructions)


## Introduction

Have you ever found a product on Amazon website, full of 5 stars reviews, but when you got the product it's totally not you expected and felt disappointed? A report from Washington Post shows that Majority of reviews in certain Amazon product categories are fraudulent or paid. Company hires professtional reviewers or robots to post glowing reviews to themselves or attack their competitiors. My project uses 100G of Amazon reviews data to detect potential fake accounts on Amazon and gives warning to them when they are reading the reviews so that shoppers will think twice before buy the product. 


## Pipeline:

The Amazon customer reviews data is stored in AWS S3. Spark reads the reviews and runs NLP preprocessing using spark Machine Learning Pipeline library including tokenization and stopwords remover. A external pretrained NLP model, Global Vector, is used for text comparison. 

Text comparison process:
1. Word embedding with pretrained Global Vector model
2. Sentense embedding with pretrained tfidf model
3. Calculate cosine similarity between each pair of review vectors 

![](./img/pipeline.png)


## Word2vec Model:

Word2vev is a well know Nature Language Model. It projects each word to a high dimensionaly vector. And I am loading the pre-trained parameters from GloVec to word2vec model, which uses billions of wikipeidia articles to train the word vector.

## Data Processing:

 The reviews data is extracted from S3 datasource. Spark ML pipeline is used for processing and cleaning the text data. The processed data are fed into NLP word2vec model and then conduct text comparison. The results are stored in Cassandra and will be queried by Flask to visualize the results.