package com.my.base.hbase

import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.{Admin, ConnectionFactory, HBaseAdmin, Put}
import org.apache.hadoop.hbase.util.Bytes

/**
  * @Author: Yuan Liu
  * @Description:
  * @Date: Created in 14:32 2019/7/15
  *
  *        Good Good Study Day Day Up
  */
// 查看表结构  describe
object HBaseCreateTable {
  def main(args: Array[String]) {
    val TABLE_NAME = "TEST_YUAN"
    val zookeeper_quorum = "bqgraphx3,bqgraphx4,bqgraphx5"
    val zookeeper_client_port = "2181"
    val hBaseConf = HBaseConfiguration.create()
    hBaseConf.set(HConstants.ZOOKEEPER_QUORUM, zookeeper_quorum)
    hBaseConf.set(HConstants.ZOOKEEPER_CLIENT_PORT, zookeeper_client_port)
    val connect = ConnectionFactory.createConnection(hBaseConf)
    val admin = connect.getAdmin
    try {
      if (admin.tableExists(TableName.valueOf(TABLE_NAME))) {
        admin.disableTable(TableName.valueOf(TABLE_NAME));
        admin.deleteTable(TableName.valueOf(TABLE_NAME));
      }
      //2\创建描述
      val h_table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
      val column = new HColumnDescriptor("BASE".getBytes());
      //column.setBlockCacheEnabled(true)
      //column.setBlocksize(2222222)
      // 添加列簇
      h_table.addFamily(column);
     // h_table.addFamily(new HColumnDescriptor("GPS".getBytes()));
      //3\创建表
      admin.createTable(h_table)
      val table = connect.getTable(TableName.valueOf(TABLE_NAME))

      //插入5条数据
      for (i <- 1 to 5) {
        // 这里是主键
        val put = new Put(Bytes.toBytes("ROW" + i))
        // 必须添加到已经存在的列簇，列名可以不存在。
        put.addColumn(Bytes.toBytes("BASE"), Bytes.toBytes("NAME"), Bytes.toBytes("value " + i))
        put.addColumn(Bytes.toBytes("BASE"), Bytes.toBytes("FAMM"), Bytes.toBytes("value " + i+"2"))
        table.put(put)
      }
      table.close()
    } catch {
      case ex: Exception => ex.printStackTrace()
    } finally {
      releaseConn(admin)
    }
  }

  def releaseConn(admin: Admin) = {
    try {
      if (admin != null) {
        admin.close();
      }
    } catch {
      case ex: Exception => ex.getMessage
    }
  }
}
