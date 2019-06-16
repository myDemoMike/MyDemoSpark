./bin/spark-shell --master yarn-client --jars /usr/local/src/apache-hive-1.2.2-bin/lib/mysql-connector-java-5.1.34.jar

create EXTERNAL TABLE lcs_test
(
    item_a STRING,
    item_b STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LINES TERMINATED BY '\n'
location '/hivedata';


import org.apache.spark.sql.functions._

def LCS(sub1: String, sub2: String): Double = {
      val opt = Array.ofDim[Int](sub1.length + 1, sub2.length + 1)
      for (i <- 0 until sub1.length reverse) {
        for (j <- 0 until sub2.length reverse) {
          if (sub1(i) == sub2(j))
            opt(i)(j) = opt(i + 1)(j + 1) + 1
          else
            opt(i)(j) = opt(i + 1)(j).max(opt(i)(j + 1))
        }
      }
      opt(0)(0) * 2 / (sub1.length + sub2.length).toDouble
}




val LCS_UDF = udf((col1: String, col2: String) => LCS(col1, col2))

val data = spark.sql("select item_a,item_b from lcs_test")

val df_res = data.withColumn("LCS_score", LCS_UDF(col("item_a"), col("item_b")))

df_res.show()

2.x中默认不支持笛卡尔积操作，需要通过参数spark.sql.crossJoin.enabled开启
spark.conf.set("spark.sql.crossJoin.enabled", "true")

val join_data = data.select("item_a").join(data.select("item_b"))

join_data.show()
val df_res2 = join_data.withColumn("LCS_score",LCS_UDF(col("item_a"),col("item_b")))





TF-IDF
create EXTERNAL TABLE news
(
  content String
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LINES TERMINATED BY '\n';

LOAD DATA LOCAL INPATH '/home/yuan/nlp_demo/allfiles.txt' OVERWRITE INTO TABLE news;


hadoop fs -rmr /spark_test_output

#/usr/local/src/spark-2.2.0-bin-hadoop2.7/bin/spark-submit --master spark://master:7077 \

/usr/local/src/spark-2.2.0-bin-hadoop2.7/bin/spark-submit --master yarn-cluster \
    --num-executors 2 \
    --executor-memory '512m' \
    --executor-cores 1 \
    --class com.my.base.nlp.nlpTest ./MySparkDemo.jar \