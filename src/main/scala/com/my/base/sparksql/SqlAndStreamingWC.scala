package com.my.base.sparksql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, SQLContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

case class Record(word: String)

object SQLContextSingleton {
    @transient  private var instance: SQLContext = _
    def getInstance(sparkContext: SparkContext): SQLContext = {
        if (instance == null) {
        instance = new SQLContext(sparkContext)
        }
        instance
    }
}

object SqlAndStreamingWC {
    def main(args: Array[String]) {
        if (args.length < 2) {
          System.err.println("Usage: NetworkWordCount <hostname> <port>")
          System.exit(1)
        }

        val sparkConf = new SparkConf().setAppName(this.getClass().getSimpleName().filter(!_.equals('$')))
        val sc = new SparkContext(sparkConf)
        val ssc = new StreamingContext(sc, Seconds(30))

        val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
        val words = lines.flatMap(_.split(" "))

        words.foreachRDD((rdd: RDD[String], time: Time) => {
            val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
            import sqlContext.implicits._
            //转换成DataFrame
            val wordsDataFrame = rdd.map(w => Record(w)).toDF()
            wordsDataFrame.registerTempTable("words")
            val wordCountsDataFrame =
                sqlContext.sql("select word, count(*) as total from words group by word")
            println(s"========= $time =========")
            wordCountsDataFrame.show()
        })

        ssc.start()
        ssc.awaitTermination()
    }
}
