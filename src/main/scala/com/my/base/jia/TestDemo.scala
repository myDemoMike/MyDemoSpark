package com.my.base.jia

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by yuan on 2018/2/8.
  */
object TestDemo {

    def main(args: Array[String]) {
      val input = args(0)

      val conf = new SparkConf().setAppName("RDD_Test").set("spark.executor.memory", "2g")
      val sc = new SparkContext(conf)

      val lines = sc.textFile(input, 4)
        .filter { x => "^\\d+$".r.findFirstMatchIn(x.split(',')(0)) != None }
        .persist(StorageLevel.MEMORY_AND_DISK_SER_2)

      // reduce
      val rdd_reduce = lines.map(_.split(',')(1).toInt).filter(_ < 100).repartition(1)
        .reduce((x, y) => x + y)
      val rdd_reduce_value = sc.parallelize(Array[Int](rdd_reduce))
      rdd_reduce_value.saveAsTextFile("hdfs://master:9000/output/rdd_reduce_value/")


      // groupByKey
      val rdd_groupByKey = lines.map(x => (x.split(',')(1), x.split(',')(0)))
        .groupByKey()
        .map(x => x._1 + "\t" + x._2.toArray.sortWith((x, y) => x.toInt < y.toInt).mkString(","))
        .sortBy(l => l.split('\t')(0).toInt, true, 1)

      rdd_groupByKey.saveAsTextFile("hdfs://master:9000/output/rdd_groupByKey/")

      // reduceByKey
      val rdd_reduceByKey = lines.map(x => (x.split(',')(1), x.split(',')(0)))
        .reduceByKey((x, y) => x + "," + y)
        .map(x => x._1 + "\t" + Sort(x._2.split(',')))
        .sortBy(l => l.split('\t')(0).toInt, true, 1)

      rdd_reduceByKey.saveAsTextFile("hdfs://master:9000/output/rdd_reduceByKey/")

      // aggregateByKey
      val rdd_aggregateByKey = lines.map(x => (x.split(',')(1), x.split(',')(0)))
        .aggregateByKey("")((x, y) => {
          x + "---" + y
        }, (x, y) => {
          x + "," + y
        })
        .map(kv => kv._1 + "\t" + Sort(kv._2.split(',')))
        .sortBy(l => l.split('\t')(0).toInt, true, 1)
      rdd_groupByKey.saveAsTextFile("hdfs://master:9000/output/rdd_aggregateByKey/")

      // join
      val some_users = lines.filter(x => x.split(',')(0).toInt % 5 == 1).map(x => (x.split(',')(0), None))
      val some_users1 = sc.parallelize(1 to 10000000).filter(x => x % 5 == 1).map(x => (x.toString, None))
      val user_orders = lines.map(x => (x.split(',')(1), x.split(',')(0)))

      val rdd_join = user_orders.join(some_users)
        .sortBy(x => x._1, true, 1)
        .map(x => x._1 + "\t" + x._2._1)
      val rdd_leftjoin = user_orders.leftOuterJoin(some_users)
        .sortBy(x => x._1, true, 1)
        .map(x => x._1 + "\t" + x._2._1)
      var rdd_rightjoin = user_orders.rightOuterJoin(some_users1)
        .sortBy(x => x._1, false, 1)
        .map(x => x._1 + "\t" + showCapital(x._2._1))

      rdd_join.saveAsTextFile("hdfs://master:9000/output/rdd_join/")
      rdd_leftjoin.saveAsTextFile("hdfs://master:9000/output/rdd_leftjoin/")
      rdd_rightjoin.saveAsTextFile("hdfs://master:9000/output/rdd_rightjoin/")

      // broadcast 类似map join，可以分散load balance
      val some_users2 = lines.filter(x => x.split(',')(0).toInt % 123 == 1).map(x => (x.toString, None))
      sc.broadcast(some_users2)
      var rdd_broadcast = user_orders.rightOuterJoin(some_users1)
        .sortBy(x => x._1, false, 1)
        .map(x => x._1 + "\t" + showCapital(x._2._1))
      rdd_rightjoin.saveAsTextFile("hdfs://master:9000/output/rdd_broadcast/")
    }

    def Sort(arr: Array[String]): String = {
      var sortArr = arr.sortWith((a, b) => a.toInt > b.toInt)
      var result = sortArr.mkString(",")
      result
    }

    def showCapital(x: Option[String]) = x match {
      case Some(s) => s
      case None => "None"
    }



}
