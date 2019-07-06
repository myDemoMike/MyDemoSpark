package com.my.base.feature

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.SparkSession

object StringIndexDemo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().config(new SparkConf().setMaster("local[*]")).getOrCreate()
    val df = spark.createDataFrame(
      Seq((0,"a"),(1,"b"),(2,"c"),(3,"a"),(4,"a"),(5,"c"))
    ).toDF("id","category")
    //创建一个StringIndexer，设置输入列为"category"，输出列为"categoryIndex"
    val indexer =new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")
    val indexed = indexer.fit(df).transform(df)
    indexed.show()

  }
}
