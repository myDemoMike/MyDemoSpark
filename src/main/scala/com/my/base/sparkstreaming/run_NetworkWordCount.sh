
#  /usr/local/src/spark-2.2.0-bin-hadoop2.7/bin/spark-submit --master local[2] \
#   --class spark.example.NetworkWordCount ./target/scala-2.11/wordcount_2.11-1.6.0.jar \
#   master \
#   9999

    /usr/local/src/spark-2.2.0-bin-hadoop2.7/bin/spark-submit --master yarn-cluster \
    --num-executors 2 \
    --executor-memory '512m' \
    --executor-cores 2 \
    --class spark.example.NetworkWordCount ./spark_streaming_demo.jar \
    master \
    9999
