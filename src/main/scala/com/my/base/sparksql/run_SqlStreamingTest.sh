#!/usr/bin/env bash
/usr/local/src/spark-2.2.0-bin-hadoop2.7/bin/spark-submit --master local[2] \
   --class spark.example.SqlTest ./spark_streaming_demo.jar \
   master \
   9999

# nohup bash run_SqlStreamingTest.sh 1>1.log 2>2.log &

# 标准输出到1.log 错误输出到2.log