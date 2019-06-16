package com.my.base

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object SparkTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("sqltest").setMaster("local[4]")
    val spark = SparkSession.builder().config(sparkConf).appName("User Base CF").getOrCreate()


    val colNames = Array("id")
    val schema = StructType(colNames.map(fieldName => StructField(fieldName, DoubleType, true)))
    var emptyDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    val m = Array(0.6, 0.4)
    for (elem <- m) {
      //-------------
      val result = test(spark, elem)
      emptyDf = emptyDf.union(result)
    }
    emptyDf.show()
  }

  def test(spark: SparkSession, input: Double): DataFrame = {
    import spark.implicits._
    val primitiveDS = Seq(input).toDF("id")
    primitiveDS
  }

}
