# !/bin/bash
spark-submit \
    --master spark://ip-10-0-0-11:7077 \
    --conf spark.executor.memoryOverhead=600 \
    --executor-memory 5G \
    --total-executor-cores 12 \
    --driver-memory 5G
    postgres.py