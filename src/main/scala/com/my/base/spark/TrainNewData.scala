package com.my.base.spark

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by yuan on 2018/1/29.
  */
object TrainNewData {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("testTrainNewData")
    val sc = new SparkContext(conf)

    val input = sc.textFile(args(0).toString)

    //val input = sc.textFile("hdfs://master:9000/train_new.data")
    val output = args(1).toString

    val UI_RDD = input.map { x =>
      val fields = x.split("\t")
      fields(0) + "\t" + fields(1) + "\t" + fields(2)
      //将第一列不为99的全部选取
      fields(0).toString != "99"
      input2(fields(0), fields(1))
    }
//    }.map { x =>
//      val fields = x.split("\t")
//      fields(0) + "\t" + fields(1)
//      val threee = fields(1)  + "\t" + fields(2)
//      println(fields(0) + "\t" + fields(1)) //进行print后  数据不会存在HDFS上
//    }
//    val LEN_INPUT = input.count()
//    println("========================" + LEN_INPUT)
//    //在终端调试   UI_RDD.take(10)
//
//    //val UI_RDD = input.foreach { x =>
//    // println(x)                      //注解在终端输出数据。
//    //}

  }
}


case class input2(f1:String,f2: String)