package com.my.base.feature

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.ChiSqSelector
import org.apache.spark.sql.SparkSession

object ChiSqSelectorDemo {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[*]")
    val spark = SparkSession
      .builder()
      .appName("ChiSqSelectorDemo Test")
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()
    val data = spark.read.format("libsvm").load("sample_libsvm_data.txt")
    val selector = new ChiSqSelector()
      .setNumTopFeatures(20)
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setOutputCol("selectedFeatures")
    val result = selector.fit(data).transform(data)
    println(s"ChiSqSelector output with top ${selector.getNumTopFeatures} features selected")
    result.drop("features").show(20,truncate = false)
  }
}
