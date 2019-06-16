package com.my.base.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WindowTest {
  def main(args: Array[String]):Unit =  {

    val sparkConf = new SparkConf().setAppName("NetworkWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    //ssc.checkpoint("hdfs://master:9000/hdfs_checkpoint")

    val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(" "))
    //窗口总长度，滑动时间间隔
    val wordCounts = words.map(x => (x, 1)).reduceByKeyAndWindow((v1: Int, v2: Int) => v1 + v2, Seconds(30), Seconds(10))
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()

    //window spark
    // 重要的2个参数：
    // 窗口长度：窗口的持续时间
    // 滑动间隔：窗口操作的间隔。
    //注意： 这两个参数必须是DStream批次间隔的倍数。


  }
}
