package com.my.base.spark

import com.huaban.analysis.jieba.JiebaSegmenter.SegMode
import com.huaban.analysis.jieba.{JiebaSegmenter, SegToken}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
  * Created by yuan on 2018/1/29.
  */
object sparkTest {
  def main(args: Array[String]): Unit = {
    val warehouselocation = "hdfs://user/hive/warehouse"
    //序列化，节省时间。
    val conf = new SparkConf().registerKryoClasses(Array(classOf[JiebaSegmenter])).set("spark.rpc.message.maxSize", "800")
    val spark = SparkSession
      .builder()
      .appName("Bayes Test")
      //支持使用hive
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", warehouselocation)
      .config(conf)
      .getOrCreate()

    import spark.implicits._
    val df = spark.sql("select word,count(1) cnt from (select explode(split(text,' ')) word from default.test)t group by word")
    print("Hive word count in spark sql")
    df.show()
    val path = "hdfs://master:9000/data/Romeo_and_Juliet.txt"
    val df2 = spark.sparkContext.textFile(path).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).toDF("word", "cnt")

    //可以直接存储，hive表
    df2.write.mode("overwrite").saveAsTable("testA")



    def jieba_cut(df: DataFrame, colname: String): DataFrame = {
      val segmenter = new JiebaSegmenter()
      val seg = spark.sparkContext.broadcast(segmenter)

      //udf
      val jieba_udf = udf { (sentence: String) =>
        val segV = seg.value
        segV.process(sentence.toString, SegMode.INDEX).
          toArray().map(_.asInstanceOf[SegToken].word).
          //单词个数大于一的取出来
          filter(x => x.length > 1)
      }
      df.withColumn(s"cut_$colname", jieba_udf(col(colname)))
    }
  }


}
