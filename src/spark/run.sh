# !/bin/bash
spark-submit \
    --conf spark.cassandra.connection.host="10.0.0.13, 10.0.0.7, 10.0.0.5" \
    --master spark://ip-10-0-0-11:7077 \
    --conf spark.executor.memoryOverhead=600 \
    --executor-memory 5G \
    --driver-memory 5G
    pyspark.py