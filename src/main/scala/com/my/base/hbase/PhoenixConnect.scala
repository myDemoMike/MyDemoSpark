package com.my.base.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession

/**
  * @Author: Yuan Liu
  * @Description: spark 写入phoenix
  * @Date: Created in 13:05 2019/7/23
  *
  *        Good Good Study Day Day Up
  */
object PhoenixConnect {

  // log4j2 使用了全局异步打印日志的方式，还需要引入disruptor的依赖
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("phoenix-test").getOrCreate()
    // 第一种读取方法
    //    val df = spark.read.format("org.apache.phoenix.spark")
    //      .option("table", "test")
    //      .option("zkUrl", "10.31.1.123,10.31.1.124,10.31.1.125:2181")
    //      .load()
    //    // 对列名的大小写不敏感，对值的大小写敏感
    //   val df2 = df.filter("mycolumn  like 'Hell%'")
    //   df2.show()
    //
    val configuration = new Configuration()
    configuration.set("hbase.zookeeper.quorum", "10.31.1.123,10.31.1.124,10.31.1.125:2181")
    // configuration.set("mapreduce.output.fileoutputformat.outputdir", "/tmp")
    // configuration.set("mapred.output.dir", "E:/hbase")
    // 第二种读取方法
    import org.apache.phoenix.spark._
    val df = spark.sqlContext.phoenixTableAsDataFrame("TEST_YUAN", Array("ROW", "FAMM", "NAME"), conf = configuration)
    df.show()

    //第一种存储方法
    // java.lang.IllegalArgumentException: Can not create a Path from an empty string   可以将spark2.2.0降为2.1.1解决问题
    // 表一定要存在
    //    df.write
    //      .format("org.apache.phoenix.spark")
    //      .mode("overwrite")
    //      .option("table", "TEST_YUAN22")
    //      .option("zkUrl", "10.31.1.123,10.31.1.124,10.31.1.125:2181")
    //      .save()

    //第二种存储方法
    df.saveToPhoenix(Map("table" -> "TEST_YUAN22", "zkUrl" -> "10.31.1.123,10.31.1.124,10.31.1.125:2181"))
  }
}
