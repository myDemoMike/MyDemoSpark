package com.my.base.hbase

import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SparkSession

/**
  * Created by 89819 on 2018/3/29.
  */
object MyTestScala {
  def main(args: Array[String]): Unit = {
    val ZOOKEEPER_QUORUM = "192.168.72.140,192.168.72.141,192.168.72.142"

    val spark = SparkSession.builder().appName("Hbase RDD").enableHiveSupport().getOrCreate()

    val rdd = spark.sql("select id,content from musics").rdd

    rdd.map { row =>
      val id = row(0).asInstanceOf[String]
      val content = row(1).asInstanceOf[String]
      // rowKey
      val p = new Put(Bytes.toBytes(id))
      //  列族。列。值。
      p.addColumn(Bytes.toBytes("content"), Bytes.toBytes("content"), Bytes.toBytes(content))
    }.foreachPartition { Iterator =>
      val jobConf = new JobConf(HBaseConfiguration.create())
      jobConf.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM)
      jobConf.set("hbase.zookeeper.property.clientPort", "2181")
      jobConf.set("zookeeper.znode.parent", "/hbase")
      jobConf.setOutputFormat(classOf[TableOutputFormat])
      val table = new HTable(jobConf, TableName.valueOf("products"))
      import scala.collection.JavaConversions._
      table.put(seqAsJavaList(Iterator.toSeq))
    }
  }
}
