
package com.my.base.hbase

import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.mapred._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SparkSession


//从hive写入hbase
object sparkRDDHbase {
  def main(args: Array[String]): Unit = {
    val ZOOKEEPER_QUORUM = "192.168.91.128,192.168.91.129,192.168.91.130"


    //hive中的数据写到Hbase。
    val spark = SparkSession.builder().appName("Hbase RDD").enableHiveSupport().getOrCreate()

    //保存到hbase  hbase 主要是提供线上的服务。
    val rdd = spark.sql("select product_id,product_name,aisle_id,department_id from badou.products where product_id='10'").rdd

    /*一个Put对象就是一行记录，在构造方法中指定主键
     * 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
     * Put.add方法接收三个参数：列族，列名，数据
     */
    rdd.map { row =>
      val product_id = row(0).asInstanceOf[String]
      val product_name = row(1).asInstanceOf[String]
      val aisle_id = row(2).asInstanceOf[String]
      val department_id = row(3).asInstanceOf[String]
      // rowKey
      val p = new Put(Bytes.toBytes(product_id))
      //  列族。列。值。
      p.addColumn(Bytes.toBytes("id"), Bytes.toBytes("aisle_id"), Bytes.toBytes(aisle_id))
      p.addColumn(Bytes.toBytes("id"), Bytes.toBytes("department_id"), Bytes.toBytes(department_id))
      p.addColumn(Bytes.toBytes("name"), Bytes.toBytes("product_name"), Bytes.toBytes(product_name))
    }.foreachPartition { Iterator =>
      //初始化jobconf，TableOutputFormat必须是org.apache.hadoop.hbase.mapred包下的！
      val jobConf = new JobConf(HBaseConfiguration.create())
      jobConf.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM)
      jobConf.set("hbase.zookeeper.property.clientPort", "2181")
      //通过zookeeper 访问。 指定datanode 访问到数据。
      jobConf.set("zookeeper.znode.parent", "/hbase")
      // 走MapReduce   OutputFormat
      jobConf.setOutputFormat(classOf[TableOutputFormat])

      val table = new HTable(jobConf, TableName.valueOf("products"))
      import scala.collection.JavaConversions._
      table.put(seqAsJavaList(Iterator.toSeq))
    }
  }
}
