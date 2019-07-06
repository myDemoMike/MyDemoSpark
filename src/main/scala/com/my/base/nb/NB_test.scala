package com.my.base.nb

import com.huaban.analysis.jieba.JiebaSegmenter
import com.my.base.jieba.JiebaSeg
import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, IDF, StringIndexer}
import org.apache.spark.sql.SparkSession


object NB_test {
  def main(args: Array[String]) = {

    val ModelPath = "/"
    //    定义结巴分词类的序列化
    val conf = new SparkConf()
      .registerKryoClasses(Array(classOf[JiebaSegmenter]))
      .set("spark.rpc.message.maxSize", "800")
    //    建立sparkSession，并传入定义好的conf
    val spark = SparkSession
      .builder()
      .appName("Bayes Test")
      .enableHiveSupport()
      .config(conf)
      .getOrCreate()


    //    从hive中取新闻数据
    val df = spark.sql("select content,label from badou.news_no_seg limit 300")

    //    调用jieba方法对新闻进行切词，选取seg（切词好的列）和对应的标签label
    val df_seg = JiebaSeg.jieba_seg(df, "content",spark).select("seg", "label")

    //    word_id 进行hashing 编码 和TF统计
    val TF = new HashingTF().setBinary(false).setInputCol("seg").setOutputCol("rawFeatures").setNumFeatures(1 << 18)
    val df_tf = TF.transform(df_seg).select("rawFeatures", "label")

    //    对word进行idf加权
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features").setMinDocFreq(1)
    val idfModel = idf.fit(df_tf)
    val df_tfidf = idfModel.transform(df_tf).select("features", "label")

    //      对string类型的label进行Doule的编码    double index向量
    val stringIndex = new StringIndexer().setInputCol("label").setOutputCol("indexed").setHandleInvalid("error")
    val df_tfidf_lab = stringIndex.fit(df_tfidf).transform(df_tfidf)

    //    对训练集和测试机进行按70%和30%进行划分
    val Array(train, test) = df_tfidf_lab.randomSplit(Array(0.7, 0.3))

    //    对贝叶斯模型进行输入和输出的定义，已经参数的定义
    val nb = new NaiveBayes()
      .setModelType("multinomial") //多项式，伯努利
      .setSmoothing(1.0)
      .setFeaturesCol("features")
      .setLabelCol("indexed")
      .setPredictionCol("pred_label")
      .setProbabilityCol("prob").setRawPredictionCol("rawPred")

    //   模型的训练
    val nbModel = nb.fit(train)

    //    模型的预测
    val pred = nbModel.transform(test)

    //   模型的F1值评估，setMetricName中可以选择{"f1", "weightedPrecision","weightedRecall", "accuracy"}
    val eval = new MulticlassClassificationEvaluator().setLabelCol("indexed").setPredictionCol("pred_label").setMetricName("f1")
    val f1_score = eval.evaluate(pred)
    println("Test f1 score = " + f1_score)

    nbModel.save(ModelPath)
    println(s"Bayes Model have saved in $ModelPath !")
  }
}
