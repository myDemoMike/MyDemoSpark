package com.my.base.feature

import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.{Row, SparkSession}

// http://spark.apache.org/docs/2.2.0/ml-tuning.html  参数选择
// https://juejin.im/post/5bc976db518825780324fd1a
// 通过配置日志文件可以选择输出最优参数
object SelectParam {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().config(new SparkConf().setMaster("local[*]")).getOrCreate()
    val training = spark.createDataFrame(Seq(
      (0L, "a b c d e spark", 1.0),
      (1L, "b d", 0.0),
      (2L, "spark f g h", 1.0),
      (3L, "hadoop mapreduce", 0.0),
      (4L, "b spark who", 1.0),
      (5L, "g d a y", 0.0),
      (6L, "spark fly", 1.0),
      (7L, "was mapreduce", 0.0),
      (8L, "e spark program", 1.0),
      (9L, "a e c l", 0.0),
      (10L, "spark compile", 1.0),
      (11L, "hadoop software", 0.0)
    )).toDF("id", "text", "label")

    // Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")
    val hashingTF = new HashingTF()
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")
    val lr = new LogisticRegression()
      .setMaxIter(10)


    // val lrStartTime = new Date().getTime

    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, hashingTF, lr))

    // 我们使用ParamGridBuilder来构建要搜索的参数网格。
    // 使用hashingTF.numFeatures的3个值和lr.regParam的2个值， regParam正则化参数
    // 此网格将有3 x 2 = 6个参数设置供CrossValidator选择。

    //    交叉验证参数设定和模型
    val paramGrid = new ParamGridBuilder()
      .addGrid(hashingTF.numFeatures, Array(10, 100, 1000))
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .addGrid(lr.elasticNetParam,Array(0.1,0.0))
      .build()

    // 模型选择与调参的三个基本组件分别是 Estimator、ParamGrid 和 Evaluator，
    // 其中Estimator包括算法或者Pipeline；
    // ParamGrid即ParamMap集合，提供参数搜索空间；
    // Evaluator 即评价指标。


    // 我们现在将Pipeline视为Estimator，将其包装在CrossValidator实例中。
    // 这将允许我们共同选择所有Pipeline阶段的参数。
    // CrossValidator需要Estimator，一组Estimator ParamMaps和一个Evaluator。
    // 请注意，此处的求值程序是BinaryClassificationEvaluator，其默认度量
    // 是areaUnderROC。
    // 交叉验证
    // BinaryClassificationEvaluator 二值数据的评估
    // RegressionEvaluator   回归
    // MulticlassClassificationEvaluator 多分类
    val cv = new CrossValidator()
      .setEstimator(pipeline)  //要优化的pipeline
      .setEvaluator(new BinaryClassificationEvaluator)  // 评价指标
      .setEstimatorParamMaps(paramGrid)   // 参数搜索
      .setNumFolds(2)  //   使用几折交叉验证

    //运行交叉验证，并选择最佳参数集。
    val cvModel = cv.fit(training)
    val bestLrModel = cvModel.bestModel.asInstanceOf[PipelineModel]
    val bestHash = bestLrModel.stages(1).asInstanceOf[HashingTF]
    val bestHashFeature = bestHash.getNumFeatures
    val bestLr = bestLrModel.stages(2).asInstanceOf[LogisticRegressionModel]
    val blr = bestLr.getRegParam
    val ble = bestLr.getElasticNetParam
    println(s"HashTF最优参数：\ngetNumFeatures= $bestHashFeature \n逻辑回归模型最优参数：\nregParam = $blr，elasticNetParam = $ble")


    // Prepare test documents, which are unlabeled (id, text) tuples.
    val test = spark.createDataFrame(Seq(
      (4L, "spark i j k","1.0"),
      (5L, "l m n","0.0"),
      (6L, "mapreduce spark","1.0"),
      (7L, "apache hadoop","0.0")
    )).toDF("id", "text","label")


    cvModel.transform(test)
      .select("id", "text", "probability", "prediction")
      .collect()
      .foreach { case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
        println(s"($id, $text) --> prob=$prob, prediction=$prediction")
      }

//    val lrvaliad = cvModel.transform(training)
//    val lrPredictions = cvModel.transform(test)
//
//    val evaluator = new BinaryClassificationEvaluator()
//      .setLabelCol("label")
//      .setRawPredictionCol("prediction")
//
//    val accuracyLrt = evaluator.evaluate(lrvaliad)
//    println(s"逻辑回归验证集分类准确率 = $accuracyLrt")
//    val accuracyLrv = evaluator.evaluate(lrPredictions)
//    println(s"逻辑回归测试集分类准确率 = $accuracyLrv")
//    val lrEndTime = new Date().getTime
//    val lrCostTime = (lrEndTime - lrStartTime)/paramGrid.length
//    println(s"逻辑回归分类耗时：$lrCostTime")

  }
}
