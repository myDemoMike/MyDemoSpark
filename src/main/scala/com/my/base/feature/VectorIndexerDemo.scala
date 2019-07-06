package com.my.base.feature

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{StringIndexer, VectorIndexer}
import org.apache.spark.sql.SparkSession

object VectorIndexerDemo {


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().config(new SparkConf().setMaster("local[*]")).getOrCreate()
    val data = spark.read.format("libsvm").load("sample_libsvm_data.txt")

    // 即只有种类小10的特征才被认为是类别型特征，否则被认为是连续型特征：
    val indexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexed")
      .setMaxCategories(10)

    val indexerModel = indexer.fit(data)


    val categoricalFeatures: Set[Int] = indexerModel.categoryMaps.keys.toSet
    // 分类的特征有多少列   连续的为总列-分类的特征
    println(s"Chose ${categoricalFeatures.size} categorical features: " +
      categoricalFeatures.mkString(", "))

    // Create new column "indexed" with categorical values transformed to indices
    val indexedData = indexerModel.transform(data)
    indexedData.show(2,truncate = false)

  }
}
