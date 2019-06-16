package com.my.base.spark

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by yuan on 2018/1/29.
  */
object WordCount {

  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.print("Usage: spark.example.WordCount <input> <output>")
      // 0 正常退出，程序正常执行结束退出
      // 1 是非正常退出，就是说无论程序正在执行与否，都退出，
      System.exit(1)
    }

    val input = args(0).toString;
    val output = args(1).toString;

    val conf = new SparkConf().setAppName("WordCount")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)

    val inputFile = sc.textFile(input)

    val countResult = inputFile.flatMap(line => line.split(" ")).map(
      word => (word, 1)).
      reduceByKey(_ + _).
      map(x => x._1 + "\t" + x._2).
      saveAsTextFile(output)
  }
}
