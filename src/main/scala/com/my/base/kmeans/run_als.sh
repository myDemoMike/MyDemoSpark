#!/usr/bin/env bash

/usr/local/src/spark-2.2.0-bin-hadoop2.7/bin/spark-submit --master yarn-cluster \
    --class spark.example.MovieLensALS ./spark_workstation.jar

