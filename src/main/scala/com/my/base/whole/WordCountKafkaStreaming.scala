package com.my.base.whole

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, HashPartitioner, SparkConf}

object WordCountKafkaStreaming {

  // key(word), value list(count), sum
  val updateFunc = (it: Iterator[(String, Seq[Int], Option[Int])]) => {
    //it.map(t => (t._1, t._2.sum + t._3.getOrElse(0)))
    it.map { case (w, s, o) => (w, s.sum + o.getOrElse(0)) }
  }

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: NetworkWordCount <hostname> <port>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("NetworkWordCount").set("spark.cores.max", "10")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    ssc.checkpoint("./local_checkpoint")

    val zkQuorum = "master:2181,slave1:2181,slave2:2181"
    val groupId = "group_1"

    // val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
    // val topicAndLine: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, zkQuorum, groupId, topic, StorageLevel.MEMORY_ONLY)
    val topicAndLine: ReceiverInputDStream[(String, String)] =
      KafkaUtils.createStream(ssc, zkQuorum, groupId, Map("wordcount" -> 2), StorageLevel.MEMORY_ONLY)

    val lines: DStream[String] = topicAndLine.map { x =>
      x._2
    }

    lines.foreachRDD(x=>{   // x RDD[String]
      x.foreachPartition(y=>{  // y iterator[String]
        y.foreach(z=>{   // z String
            z
        })
      })
    })

    val words = lines.flatMap(_.split(" "))
    //val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    val wordCounts = words.map(x => (x, 1)).updateStateByKey(updateFunc, new HashPartitioner(ssc.sparkContext.defaultParallelism), true)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
