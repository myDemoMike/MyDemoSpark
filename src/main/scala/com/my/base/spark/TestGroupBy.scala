package com.my.base.spark

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by yuan on 2018/1/29.
  */
object TestGroupBy {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("testTrainNewData")
    val sc = new SparkContext(conf)
    val input = sc.textFile(args(0).toString)

    val output = args(1).toString

    input.filter { x =>
      val fields = x.split("\t")
      fields(0) != "99"
    }.map { line =>
      val fields = line.split("\t")
      (fields(0), (fields(1), fields(2)))
    }.groupByKey().map { line =>
      val user_id = line._1
      val value_list = line._2
      val value_arr = value_list.toArray
      val len = value_arr.length
      val buf = new StringBuilder()
      for (i <- 0 until len - 1) {
        buf ++= value_arr(i)._1
        buf.append(".")
      }
      buf ++= value_arr(len - 1)._1
      user_id + "\t" + buf
    }.saveAsTextFile(output)
  }

  //输出 91  1000022.1000031.1000050.1000056.1000069.1000079.1000098.10000127.10000132.10000135.10000136.10000143.10000176.10000182.10000183.10000187.10000192.10000204.10000205.10000210.10000211.10000294.10000300.10000328.10000333.10000343.10000389.10000418.10000429.10000474.10000483.10000495.10000498.10000501.10000511.10000520.10000527.10000568.10000601.10000603.10000612.10000614.10000651.10000657.10000662.10000735.10000750.10000988.100001126.100001192

}
