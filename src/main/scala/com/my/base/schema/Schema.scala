package com.my.base.schema

import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by zheng on 2018/1/25.
  */
object Schema {
  val NewKeyWordSchema = StructType(Array(
    StructField("word", StringType),
    StructField("label", StringType),
    StructField("dt", StringType)
  ))

  val WrongNewKeyWordSchema = StructType(Array(
    StructField("errorlog", StringType),
    StructField("exceptiontype", StringType),
    StructField("dt", StringType)
  ))
}
