package com.my.base.sparksql

import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object SqlTest {
  def main(args: Array[String]) {

    val StudentSchema: StructType = StructType(mutable.ArraySeq(
      StructField("Sno", StringType, nullable = false),
      StructField("Sname", StringType, nullable = false),
      StructField("Ssex", StringType, nullable = false),
      StructField("Sbirthday", StringType, nullable = true),
      StructField("SClass", StringType, nullable = true)
    ))

    val sparkConf = new SparkConf().setAppName("sqltest")
    val spark = SparkSession.builder().appName("User Base CF").enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    val m = Array(0.6,0.4)
    //dataFrame的引入
    val sqlContext = spark.sqlContext

    //  val sc = SparkSession.builder().appName("my spark").enableHiveSupport().getOrCreate()
    val StudentData = sc.textFile("hdfs://master:9000/sql_stu.data").map {
      lines =>
        val line = lines.split(",")
        Row(line(0), line(1), line(2), line(3), line(4))
    }


    /*      val spark = SparkSession
            .builder()
            .appName("Bayes Test")
            //支持使用hive
            .enableHiveSupport()
            .config("spark.sql.warehouse.dir", warehouselocation)
            .config(conf)
            .getOrCreate()*/


    val StudentTable = sqlContext.createDataFrame(StudentData, StudentSchema)
    StudentTable.createOrReplaceTempView("Student")
    sqlContext.sql("SELECT Sname, Ssex, SClass FROM Student").show()
  }
}
