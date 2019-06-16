package com.my.base.nlp

import breeze.numerics.log
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object nlpTest {
  def main(args: Array[String]): Unit = {
    val warehouselocation = "hdfs://user/hive/warehouse"
    val spark = SparkSession
      .builder()
      .appName("Bayes Test")
      .enableHiveSupport()
      //spark.sql.warehouse.dir 配置指定数据库的默认存储路径。
      .config("spark.sql.warehouse.dir", warehouselocation)
      .getOrCreate()

    //lcs
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

    def LCS_Transform(): Unit = {
      //1.LCS
      val data = spark.sql("select item_a,item_b from default.lcs_test")
      val df_res = data.withColumn("LCS_score", LCS_UDF(col("item_a"), col("item_b")))
      df_res.show()
      df_res.write.mode("overwrite").saveAsTable("default.lcs_result")

      //2.笛卡儿积
      val join_data = data.select("item_a").join(data.select("item_b"))
      join_data.show()
      val df_res2 = join_data.withColumn("LCS_score", LCS_UDF(col("item_a"), col("item_b")))
      df_res2.show()
    }

    //TF-IDF
    def TFIDF_Test(): Unit = {
      //    tfidf
      val data = spark.sql("select content from default.news_no_seg")
      import spark.implicits._
      //    统计文档总数
      val total_doc = data.count()
      //    统计词频  toDF 做引式转换。
      val df_tf = data.map(_.toString().split("##@@##")(0).split(" ")).groupBy("value").count().toDF("word", "tf")
      //    统计包含该词的文档数
      val df_df = data.flatMap(_.toString().split("##@@##")(0).split(" ").toSet).groupBy("value").count().toDF("word", "df")
      //    tfidf公式
      val tfidf_udf = udf((tf: Int, df: Int) => tf * log(total_doc / (df.toDouble + 1.0)))

      val df_tfidf = df_tf.join(df_df, df_tf("word") === df_df("word")).drop(df_df("word")).withColumn("tfidf_score", tfidf_udf(col("tf"), col("df")))
      df_tfidf.write.mode("overwrite").saveAsTable("default.tfidf_result")
    }
    TFIDF_Test()
  }


}