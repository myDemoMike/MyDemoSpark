package com.my.base.jieba

import com.huaban.analysis.jieba.JiebaSegmenter.SegMode
import com.huaban.analysis.jieba.{JiebaSegmenter, SegToken}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, udf}


object JiebaSeg {

  //    定义结巴分词的方法，传入的是DataFrame，输出的DataFrame会多一列seg（即分好词的一列）
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestJieBa")
      //      .set("spark.rpc.message.maxsize", "800")
      .setMaster("local")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[JiebaSegmenter]))

    val spark = SparkSession.builder().appName("My_Jieba")
      .enableHiveSupport()
      .config(conf)
      .getOrCreate()
    val df = spark.sql("select content,label from news_no_seg limit 200")
    df.show()
    val df_seg = jieba_seg(df, "content", spark).select("seg", "label")
    df_seg.show()
  }

  def jieba_seg(df: DataFrame, colname: String, spark: SparkSession): DataFrame = {
    val segmenter = new JiebaSegmenter()
    val seg = spark.sparkContext.broadcast(segmenter)
    val jieba_udf = udf { (sentence: String) =>
      val segV = seg.value
      segV.process(sentence.toString, SegMode.INDEX)
        .toArray()
        .map(_.asInstanceOf[SegToken].word)
        .filter(_.length > 1)
    }
    df.withColumn("seg", jieba_udf(col(colname)))
  }
}
