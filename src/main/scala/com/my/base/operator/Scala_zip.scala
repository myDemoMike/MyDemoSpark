package com.my.base.operator

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by 89819 on 2018/3/5.
  */
object Scala_zip {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("RDD_Test").set("spark.executor.memory", "2g").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val a = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"), 3)
    val b = a.map{_.length}
    val c = a.zip(b)
    c.collect.foreach(println)

  }
}
