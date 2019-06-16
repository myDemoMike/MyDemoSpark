package com.my.base.scala

/**
  * Created by yuan on 2017/12/20.
  */
object Test_str {
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
