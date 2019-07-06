package com.my.base.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by 89819 on 2018/3/4.
  */
object AggregateTest {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("ALS Train").enableHiveSupport().getOrCreate()
    val data = spark.sparkContext.parallelize(List(2, 5, 8, 1, 2, 6, 9, 4, 3, 5), 2)
    println("数据的分区数" + data.getNumPartitions)
    val res = data.aggregate((0, 0))( //初始值定义了返回的数据类型
      //seqOp把值number合并到数据结构acc， 该函数在分区内合并值时使用
      (acc, number) => (acc._1 + number, acc._2 + 1),
      //合并两个数据结构par1，在分区间合并值时调用此函数。
      (par1, par2) => (par1._1 + par2._1, par1._2 + par2._2)
    )
    println(res)

    //先對每個分區裡面的做第一個  然後合併做第二個操作    怎么确定每个哪个数据到哪个分区呢
    val rdd2 = spark.sparkContext.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8), 2)
    val res2 = rdd2.aggregate(5)(math.max(_, _), _ + _)
    println(res2) //29

    val data2 = spark.sparkContext.parallelize(List((1, 3), (1, 2), (1, 4), (2, 3), (3, 6), (3, 8)))
    println(data2.getNumPartitions)
    val res3: RDD[(Int, Int)] = data2.aggregateByKey(0)(
      // seqOp
      math.max(_, _),
      // combOp
      _ + _
    )

    res3.collect.foreach(println)
  }
}
