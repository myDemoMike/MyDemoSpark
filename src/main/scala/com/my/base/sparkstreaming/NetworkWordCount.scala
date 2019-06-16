package com.my.base.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object NetworkWordCount {

  def main(args: Array[String]) {

    if (args.length < 2) {
      System.err.println("Usage: NetworkWordCount <hostname> <port>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("NetworkWordCount")
    // 每5秒钟做数据的批处理
    // Seconds(1)代表输入的数据每一秒钟打一个batch包。一个batch包内可以有多个block，一个block对应一个task
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    // 不叫RDD 叫DStream
    // 通过Socket获取数据，该处需要提供Socket的主机名和端口号，数据保存在内存和硬盘中
    val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(" ")) //一句话变成多个单词
    val wordss = ""
    "aaaa" -> wordss
    // val words = lines.flatMap { line =>
    //    line.split(" ")
    // }
    val wordCounts = words.map { x => (x, 1) }.reduceByKey(_ + _)
    // 输出到终端上
    wordCounts.print()
    // 写到HDFS   会输出多个文件。目录后面追加后缀  中间会有个时间戳
    wordCounts.saveAsTextFiles("hdfs://master:9000/spark_streaming_output", "mp3")
    //启动
    ssc.start()
    //监听
    ssc.awaitTermination()
  }
}
