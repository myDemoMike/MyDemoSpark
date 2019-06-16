#!/usr/bin/env bash

hadoop fs -rmr /spark_test_output

#/usr/local/src/spark-2.2.0-bin-hadoop2.7/bin/spark-submit --master spark://master:7077 \

/usr/local/src/spark-2.2.0-bin-hadoop2.7/bin/spark-submit --master yarn-cluster \
    --num-executors 2 \
    --executor-memory '512m' \
    --executor-cores 1 \
    --class com.my.base.spark.TestGroupBy ./MySparkDemo.jar \
    hdfs://master:9000/train_new.data \
    hdfs://master:9000/spark_test_output

