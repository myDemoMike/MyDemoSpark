package com.my.base.schema

import org.apache.spark.sql.types.{StringType, StructField, StructType}


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
