package com.my.base.repair

/**
  * @Author: Yuan Liu
  * @Description:
  * @Date: Created in 16:17 2019/12/13
  *
  *        Good Good Study Day Day Up
  */
object OptionDemo {

  def main(args: Array[String]): Unit = {
    val map1 = Map("key1" -> "value1")
    val value1 = map1.get("key1")
    val value2 = map1.get("key2")
    printContentLength(value1)
    printContentLength(value2)
    // value1.map(_.length).map("length: " + _).foreach(println)
    // println(showCapital(value2))
  }


  def printContentLength(x: Option[String]) {
    for (c <- x) {
      println(c.length)
    }
  }

  def showCapital(x: Option[String]) = x match {
    case Some(s) => s
    case None => "?"
  }


}
