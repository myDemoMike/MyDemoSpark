#!/usr/bin/env bash

/usr/local/src/spark-2.2.0-bin-hadoop2.7/bin/spark-submit --master yarn-cluster \
    --num-executors 1 \
    --executor-memory '512m' \
    --executor-cores 1 \
    --class com.my.base.spark.WordCount ./MySparkDemo.jar \
    hdfs://master:9000/The_Man_of_Property.txt \
    hdfs://master:9000/spark_word_count_output

