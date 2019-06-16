
package com.my.base.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object test {
  def main(args: Array[String]): Unit = {

    // 本地模式运行,便于测试
    val sparkConf = new SparkConf().setMaster("local").setAppName("HBaseTest")

    // 创建hbase configuration
    val hBaseConf = HBaseConfiguration.create()
    hBaseConf.set(TableInputFormat.INPUT_TABLE, "products")
    // 创建 spark context
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // 从数据源获取数据
    val hbaseRDD = sc.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

    // 将数据映射为表  也就是将 RDD转化为 dataframe schema
    val productDF = hbaseRDD.map(r => (
      Bytes.toString(r._2.getRow()),
      Bytes.toString(r._2.getValue(Bytes.toBytes("name"), Bytes.toBytes("product_name"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("id"), Bytes.toBytes("aisle_id"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("id"), Bytes.toBytes("department_id")))
      )).toDF("product_id", "product_name", "aisle_id", "department_id")

  }
}

