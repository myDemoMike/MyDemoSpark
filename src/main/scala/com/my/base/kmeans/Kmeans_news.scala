package com.my.base.kmeans

import com.huaban.analysis.jieba.JiebaSegmenter
import com.my.base.jieba.JiebaSeg
import org.apache.spark.SparkConf
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{HashingTF, IDF, RFormula}
import org.apache.spark.sql.SparkSession


object Kmeans_news {
  def main(args: Array[String]): Unit = {
    //    定义结巴分词类的序列化
    val conf = new SparkConf()
      .registerKryoClasses(Array(classOf[JiebaSegmenter]))
      //Spark节点间传输的数据过大，超过系统默认的128M，因此需要提高spark.rpc.message.maxSize的大小或者选择用broadcast广播数据。
      .set("spark.rpc.message.maxSize", "800")
    //  建立sparkSession，并传入定义好的conf
    val spark = SparkSession
      .builder()
      .appName("Bayes Test")
      .enableHiveSupport()
      .config(conf)
      //getOrCreate()：有就拿过来，没有就创建，类似于单例模式
      .getOrCreate()

    val df = spark.sql("select content from test.news_no_seg limit  300")
    val df_seg = JiebaSeg.jieba_seg(df, "content", spark)

    //    word_id 进行hashing 编码 和TF统计
    //  # 切词之后的文本特征的处理   是否使用伯努利
    val TF = new HashingTF().setBinary(false).setInputCol("seg").setOutputCol("rawFeatures").setNumFeatures(1 << 18)
    val df_tf = TF.transform(df_seg).select("rawFeatures")

    //    对word进行idf加权
    //
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features").setMinDocFreq(1)
    val idfModel = idf.fit(df_tf)
    val df_tfidf = idfModel.transform(df_tf).select("features")
    /**
      * 传入的是“features”这一列，输出增加聚完类的“pred”这一列
      * 初始化k个点是用Kmeans++，参数“k-means||”
      * 设置类别K10个
      * Seed为随机种子
      **/
    val kmeans = new KMeans()
      .setFeaturesCol("features")
      .setInitMode("k-means||")
      // 同时训练5个模型，选择一个模型  作为输出
      .setInitSteps(5)
      .setK(10) //k簇的个数
      .setMaxIter(20)
      .setPredictionCol("pred")
      .setSeed(1234)

    val formula = new RFormula()
      .setFormula("clicked ~ country + hour+count")
      .setFeaturesCol("features")

    val model = kmeans.fit(df_tfidf)
    val WSSSE = model.computeCost(df_tfidf)
    print(s"Within Set Sum of Squared Errors = $WSSSE")

    print("Cluster Centers:")
    model.clusterCenters.foreach(println)
  }

}
