package com.my.base.sparkstreaming

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.my.base.LogGen
import com.my.base.schema.Schema
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


object StreamFromKafka {
  def main(args: Array[String]): Unit = {



    if (args.length < 3) {
      System.err.print("Usage: Collect log from Kafka <groupid> <topic> <Execution_time>")
      System.exit(1)
    }

    val Array(group_id, topic, exectime, dynamic) = args
    val ZK_QUORUM = "192.168.226.143:2181,192.168.226.144:2181,192.168.226.145:2181"

    val numThreads = 1
    val conf = new SparkConf()
    conf.set("","")
    val ssc = new StreamingContext(conf, Seconds(exectime.toInt))


    //传进去的topic 可以能为多个
    val topicSet = topic.split(",").toSet
    val topicMap = topicSet.map((_, numThreads.toInt)).toMap

    //（1）如果数据量少的话，我们需要通过repartition（1），
    //（2）JSON根据我们定义好的数据结构去解析
    //（3）重点：如果有几条数据有问题，影响整个spark streaming任务，怎么解决
    //（4）返回需要ROW()，需要我们定义的class的get方法取到我们想要的数据放到Row里面

    //      通过Receiver接收kafka数据
    val mesR = KafkaUtils.createStream(ssc, ZK_QUORUM, group_id, topicMap).map(_._2)

    //产生的文件数量变少
    val log = mesR.repartition(1).map { x =>
      try {
        val mesJson = JSON.parseObject(x, classOf[LogGen])
        //scala 返回的是最后一句。
        ("right", log2Row(mesJson))
      } catch {
        case ex: Exception => ("wrong", Row(x, ex.toString))
      }
    }

    log.filter(_._1 == "right").map(_._2)
      .foreachRDD { rdd =>
        if (dynamic.toInt == 1) rddSave(rdd, Schema.NewKeyWordSchema, "badou.fact_log_static")
        else rddSaveTable(rdd, Schema.NewKeyWordSchema, "badou.fact_log")
      }
    // 解析有问题的数据放入另外表
    log.filter(_._1 == "wrong").map(_._2)
      .foreachRDD { rdd =>
        val dt = getNowDate()
        val rdd1 = rdd.map(r => Row(r.getString(0), r.getString(1), dt))
        if (dynamic.toInt == 1) rddSave(rdd1, Schema.WrongNewKeyWordSchema, "badou.error_fact_log_static")
        else rddSaveTable(rdd1, Schema.WrongNewKeyWordSchema, "badou.error_fact_log")
      }
    ssc.start()
    ssc.awaitTermination()
  }

  def rddSave(rdd: RDD[Row], schema: StructType, tableName: String) {
    val records = rdd.coalesce(1)
    val spark = SparkSession
      .builder()
      .appName("Streaming Form Kafka Static")
      .enableHiveSupport()
      .getOrCreate()
    val df = spark.createDataFrame(records, schema)
    df.write.mode(SaveMode.Append).saveAsTable(tableName)
  }

  def rddSaveTable(rdd: RDD[Row], schema: StructType, tableName: String) {
    val records = rdd.coalesce(1)
    //        打开hive动态分区和非严格模式
    val spark = SparkSession.builder()
      //      .config("spark.sql.warehouse.dir","hdfs:///user/hive/warehouse/badou.db")
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .enableHiveSupport()
      .getOrCreate()
    val df = spark.createDataFrame(records, schema)
    df.write.mode(SaveMode.Append).insertInto(tableName)
  }


  def getNowDate() = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    val today = dateFormat.format(now)
    today
  }

  def log2Row(log: LogGen): Row = {
    val dt = getNowDate()
    if (log.getWord != null) {
      Row(log.getWord, log.getLabel, dt)
    } else {
      Row(null, null, dt)
    }
  }

  //6.存表：
  // （1）创建一个表，一直往里面追加数据 （只有一个表，随着时间，表内数据越来越多） fact_log(一个文件夹)

  // （2）创建一个分区表，按照dt日期做partition的（一个表，多个分区，每天spark streaming来的数据会在一天中的分区中，其他的日子，会在其他的分区中） fact_log/20180127 里面（跑了多少天，就会有多少个文件夹）

  // 重点：打开hive动态分区和非严格模式
}
