package com.my.base

import breeze.numerics.log
import com.huaban.analysis.jieba.JiebaSegmenter.SegMode
import com.huaban.analysis.jieba.{JiebaSegmenter, SegToken}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala._
/**
  * Created by zheng on 2017/12/17.
  */
object Test {
/*  def main(args: Array[String]): Unit = {
    val warehouselocation = "hdfs://user/hive/warehouse/badou.db"
    val conf = new SparkConf()
      //需要在使用时注册需要序列化的类型
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[JiebaSegmenter]))
      .set("spark.rpc.message.maxSize","800")

    val spark = SparkSession
      .builder()
      .appName("Bayes Test")
      .enableHiveSupport()
      //      .config("spark.sql.warehouse.dir", warehouselocation)
      .config(conf)
      .getOrCreate()

    import spark.implicits._
    //    val df = spark.sql("select word,count(1) cnt from (select explode(split(text,' ')) word from badou.test)t group by word")
    //
    //    df.show()
    //    val path = "hdfs://master:9000/data/Romeo_and_Juliet.txt"
    //    val df2 = spark.sparkContext.textFile(path).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).toDF("word", "cnt")
    //    df2.show()

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

    import spark.implicits._
    val LCS_UDF = udf((col1: String, col2: String) => LCS(col1, col2))

    def LCS_Transform(): Unit = {
      val data = spark.sql("select item_a,item_b from badou.lcs_test")
      val df_res = data.withColumn("LCS_score", LCS_UDF(col("item_a"), col("item_b")))
      df_res.show()
      df_res.write.mode("overwrite").saveAsTable("badou.lcs_result")
    }


    def TFIDF_Test(): Unit = {
      //    tfidf
      val data = spark.sql("select content from badou.news")
      //    统计文档总数
      val total_doc = data.count()
      val idf = udf((df: Int) => log(total_doc / (df.toDouble + 1.0)))
      //    统计词频
      val df_tf = data.flatMap(x=>x(0).toString().split("##@@##")(0).split(" ")).groupBy("value").count().toDF("word", "tf")
      //    统计包含该词的文档数
//      val df_df = df.flatMap(x=>x(0).toString().split(" ").toSet).groupBy("value").count().withColumn("df",idf(col("count"))).select("value", "df").toDF("word", "df")

      val df_df = data.flatMap(x=>x(0).toString.split("##@@##")(0).split(" ").toSet).groupBy("value").count().withColumn("df", idf(col("count"))).select("value", "df").toDF("word", "df").filter(col("df") > 3)
//      生成idf过滤后的字典map
      val df_idfMap = df_df.rdd.map(x => (x(0).toString, x(1))).collectAsMap()
      val dict_size = df_idfMap.size
      var index = -1
      import spark.implicits._
      val indexerMap = mutable.HashMap.empty[String,Int]
      df_idfMap.keys.foreach { key =>
        index = index + 1
        indexerMap.put(key,index)
      }

      def TF_transform(doc: String): Vector ={
        val document = doc.split(" ")
        val termFreqMap = mutable.HashMap.empty[Int,Double]
        val setTF = (index:Int) =>termFreqMap.getOrElse(index,0.0)+1.0
        document.foreach{term =>
          val index = indexerMap.getOrElse(term,-1)
          if(index!= -1)
            termFreqMap.put(index,setTF(index))
        }
        Vectors.sparse(dict_size,termFreqMap.toSeq)
      }
      val TF_transform_udf = udf{(doc:String)=>TF_transform(doc)}
      val df_res = data.withColumn("tf_vector",TF_transform_udf(col("content")))
      //    tfidf公式
      val tfidf_udf = udf((tf: Int, df: Int) => tf * log(total_doc / (df.toDouble + 1.0)))
      val df_tfidf = df_tf.join(df_df, df_tf("word") === df_df("word")).drop(df_df("word")).withColumn("tfidf_score", tfidf_udf(col("tf"), col("df")))
      df_tfidf.write.mode("overwrite").saveAsTable("badou.tfidf_result")
    }

    def jieba_seg(df:DataFrame,colname:String): DataFrame ={
      val segmenter = new JiebaSegmenter()
      val seg = spark.sparkContext.broadcast(segmenter)
      val jieba_udf = udf{(sentence:String)=>
        val segV = seg.value
        segV.process(sentence.toString,SegMode.INDEX)
          .toArray()
          .map(_.asInstanceOf[SegToken].word)
          .filter(_.length>1)
      }
      df.withColumn("seg",jieba_udf(col(colname)))
    }

//    LCS_Transform()
    //    TFIDF_Test()
    val df_seg = jieba_seg(spark.sql("select sentence from badou.seg"),"sentence")
    df_seg.show()
    val tf = new HashingTF()

  }*/
}
