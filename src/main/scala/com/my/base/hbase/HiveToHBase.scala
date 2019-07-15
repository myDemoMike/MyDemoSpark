package com.my.base.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable, Put}
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, HTableDescriptor, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.util.Try

/**
  * @Author: Yuan Liu
  * @Description:
  * @Date: Created in 16:27 2019/7/15
  *
  *        Good Good Study Day Day Up
  */
object HiveToHBase {
  def main(args: Array[String]): Unit = {
    val zookeeper_quorum = "bqbpm2.bqjr.cn,bqbpm1.bqjr.cn,bqbps2.bqjr.cn"
    val zookeeper_client_port = "2181"
    val TABLE_NAME = "test_yuan"
    val sparkConf = new SparkConf().setAppName("HiveToHBase")
      .setMaster("local[*]")
    val ssc = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val dataFrame = ssc.sql("select mobiletelephone,customername,address,gps,geohash from graph.bqjr_user_07_10 where mobiletelephone is not null limit 10")
    dataFrame.show(10)
    dataFrame.rdd.map(x => {
      val phone = Try(x(0).asInstanceOf[String]).getOrElse("0")
      val name = Try(x(1).asInstanceOf[String]).getOrElse("")
      val address =Try(x(2).asInstanceOf[String]).getOrElse("")
      val gps =Try(x(3).asInstanceOf[String]).getOrElse("")
      val geohash =Try(x(4).asInstanceOf[String]).getOrElse("")
      // rowkey
      // 列簇、列、值
      val p = new Put(Bytes.toBytes(phone))
      p.addColumn(Bytes.toBytes("base"), Bytes.toBytes("name"), Bytes.toBytes(name))
      p.addColumn(Bytes.toBytes("base"), Bytes.toBytes("address"), Bytes.toBytes(address))
      p.addColumn(Bytes.toBytes("gps"), Bytes.toBytes("gps"), Bytes.toBytes(gps))
      p.addColumn(Bytes.toBytes("gps"), Bytes.toBytes("geohash"), Bytes.toBytes(geohash))
    }).foreachPartition(Iterator => {
      //初始化jobconf，TableOutputFormat必须是org.apache.hadoop.hbase.mapred包下的！
      val jobConf = new JobConf(HBaseConfiguration.create())
      jobConf.set("hbase.zookeeper.quorum", zookeeper_quorum)
      jobConf.set("hbase.zookeeper.property.clientPort", zookeeper_client_port)
      //通过zookeeper 访问。 指定datanode访问到数据。
      // jobConf.set("zookeeper.znode.parent", "/hbase")
      // 走MapReduce   OutputFormat
      jobConf.setOutputFormat(classOf[TableOutputFormat])
      val table = new HTable(jobConf, TableName.valueOf(TABLE_NAME))
      import scala.collection.JavaConversions._
      table.put(seqAsJavaList(Iterator.toSeq))
    })
  }

}
