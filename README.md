# FakeOUT

## Introduction

Have you ever found a product on Amazon website, full of 5 stars reviews, but when you got the product it's totally not you expected and felt disappointed? A report from Washington Post shows that Majority of reviews in certain Amazon product categories are fraudulent or paid. Company hire professtional reviewers or robot to post glowing reviews to themselves or attack their competitiors. My project uses 100G of Amazon reviews data to detect potential fake accounts on Amazon and gives warning to them when they are reading the reviews.


## Data Pipeline:

![](./img/pipeline.png)

## Data Processing:

 The event data is extracted from S3 datasource and cleansed. The data is transformed to perform calculation of critical statistics and loaded to Cassandra. The flask web layer servers the analytics to display the frontend.