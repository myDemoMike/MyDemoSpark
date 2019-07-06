package com.my.base.operator

/**
  * Created by yuan on 2017/12/20.
  */
object Scala_str {
  def main(args: Array[String]) {
    var buf = new StringBuilder; //StringBuilder

    buf += 'a'
    println("buf value is :" + buf.toString())

    buf ++= "cdef"
    println("buf value is :" + buf.toString())

    buf.append("dsfsdfsdfds")
    println("buf value is :" + buf.toString())

  }
}
