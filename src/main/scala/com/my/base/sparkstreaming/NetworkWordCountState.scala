package com.my.base.sparkstreaming

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf}

object NetworkWordCountState {

  //更新函数。  ---Option 可选元素，状态的记忆。
  // key(word), value list(count), sum
  val updateFunc = (it: Iterator[(String, Seq[Int], Option[Int])]) => {
    it.map(t => (t._1, t._2.sum + t._3.getOrElse(0)))

    //it.map {case (w, s, o) => (w, s.sum + o.getOrElse(0))}
  }


  //带状态的流计算
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: NetworkWordCount <hostname> <port>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("NetworkWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.checkpoint("./local_checkpoint")

    val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(" "))
    //val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    //有状态  有记忆功能
    //更新参数。Partition。是否记忆，是否更新。
    val wordCounts = words.map(x => (x, 1)).updateStateByKey(updateFunc,
      new HashPartitioner(ssc.sparkContext.defaultParallelism), true)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
