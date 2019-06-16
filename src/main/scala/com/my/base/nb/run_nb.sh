#!/usr/bin/env bash

/usr/local/src/spark-2.2.0-bin-hadoop2.7/bin/spark-submit --master yarn-cluster \
    --num-executors 2 \
    --executor-memory '1024m' \
    --executor-cores 1 \
    --class com.my.base.nb.naiveBayes ./MyDemoSpark.jar \
    hdfs://master:9000/a.txt

