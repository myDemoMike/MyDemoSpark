package com.my.base.hbase

import com.bqjr.bigdata.util.ConfigUtil
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.rdd.EsSpark

/**
  * @Author: Yuan Liu
  * @Description:
  * @Date: Created in 13:49 2019/7/15
  *
  *        Good Good Study Day Day Up
  */
/**
  * HBase 1.2.0;spark 2.2.0;es 6.5.1
  */
object HbaseToES {
  def main(args: Array[String]): Unit = {
    val zookeeper_quorum = "bqbpm2.bqjr.cn,bqbpm1.bqjr.cn,bqbps2.bqjr.cn"
    val zookeeper_client_port = "2181"
    val config = ConfigUtil.getConfig
    val sparkConf = new SparkConf().setAppName("HbaseToES")
      .set("es.nodes", config.getString("app.es.ips"))
      .set("es.port", config.getString("app.es.port"))
      .set("es.index.auto.create", "true")
      .set("es.net.http.auth.user", config.getString("app.es.es_user_name"))
      .set("es.net.http.auth.pass", config.getString("app.es.es_user_pass"))

    val ssc = SparkSession.builder().appName("SparkFromHBase").master("local[*]").config(sparkConf).getOrCreate()
    val sc = ssc.sparkContext

    val tableName = "test_yuan"
    val hBaseConf = HBaseConfiguration.create()
    hBaseConf.set(HConstants.ZOOKEEPER_QUORUM, zookeeper_quorum)
    hBaseConf.set(HConstants.ZOOKEEPER_CLIENT_PORT, zookeeper_client_port)
    hBaseConf.set(org.apache.hadoop.hbase.mapreduce.TableInputFormat.INPUT_TABLE, tableName)


    //读取数据并转化成rdd TableInputFormat是org.apache.hadoop.hbase.mapreduce包下的
    val hbaseRDD = sc.newAPIHadoopRDD(hBaseConf, classOf[org.apache.hadoop.hbase.mapreduce.TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    val result = hbaseRDD.map(x => x._2).map { result =>
        (result.getRow,
          result.getValue(Bytes.toBytes("base"), Bytes.toBytes("name")),
          result.getValue(Bytes.toBytes("base"), Bytes.toBytes("address")),
          result.getValue(Bytes.toBytes("gps"), Bytes.toBytes("geohash")))
      }.map(row => testInsert(new String(row._1), new String(row._2), new String(row._3), new String(row._4)))
    println("数据量 " + result.count())
    //result.take(10).foreach(println)

    EsSpark.saveToEs(result, "test/hbase")
  }

  case class testInsert(row_id: String,
                        name: String,
                        address: String,
                        geohash: String)

}
