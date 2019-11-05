package com.my.base.feature

import org.apache.spark.SparkConf
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.sql.SparkSession

// http://spark.apache.org/docs/2.2.0/ml-tuning.html  参数选择
// https://juejin.im/post/5bc976db518825780324fd1a
// 通过配置日志文件可以选择输出最优参数
object SelectParamLinearRegression {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().config(new SparkConf().setMaster("local[*]")).getOrCreate()
    val data = spark.read.format("libsvm").load("./sample_linear_regression_data.txt")
    val Array(training, test) = data.randomSplit(Array(0.9, 0.1), seed = 12345)
    // lr.explainParams
    val lr = new LinearRegression()

    val paramGrid = new ParamGridBuilder().
      addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0)).
      //  截距
      addGrid(lr.fitIntercept).
      addGrid(lr.regParam, Array(0.1, 0.01)).
      build()

    val trainValidationSplit = new TrainValidationSplit().
      setEstimator(lr).
      setEstimatorParamMaps(paramGrid).
      setEvaluator(new RegressionEvaluator).
      setTrainRatio(0.8)

    val model = trainValidationSplit.fit(training)

    model.transform(test).select("features", "label", "prediction").show()

  }
}
