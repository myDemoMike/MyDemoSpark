
/usr/local/src/spark-1.6.0-bin-hadoop2.6/bin/spark-submit --master local[2] \
   --class spark.example.WordCountKafkaStreaming ./target/scala-2.11/wordcount_2.11-1.6.0.jar \
   master \
   9999

# /usr/local/src/spark-1.6.0-bin-hadoop2.6/bin/spark-submit --master yarn-client \
#    --num-executors 2 \
#    --executor-memory '512m' \
#    --executor-cores 2 \
#    --class spark.example.NetworkWordCountState ./target/scala-2.11/wordcount_2.11-1.6.0.jar \
#    master \
#    9999
