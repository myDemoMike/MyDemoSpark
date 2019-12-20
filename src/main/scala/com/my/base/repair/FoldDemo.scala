package com.my.base.repair

/**
  * @Author: Yuan Liu
  * @Description:
  * @Date: Created in 13:40 2019/12/16
  *
  *        Good Good Study Day Day Up
  */
object FoldDemo {

  def main(args: Array[String]): Unit = {
    val left = List(1,2,3)
    val right = List(4,5,6)

    //以下操作等价
    left ++ right   // List(1,2,3,4,5,6)
    left ++: right  // List(1,2,3,4,5,6)
    right.++:(left)    // List(1,2,3,4,5,6)
    right.:::(left)  // List(1,2,3,4,5,6)

    //以下操作等价
    0 +: left    //List(0,1,2,3)
    left.+:(0)   //List(0,1,2,3)

    //以下操作等价
    left :+ 4    //List(1,2,3,4)
    left.:+(4)   //List(1,2,3,4)

    //以下操作等价
    0 :: left      //List(0,1,2,3)
    left.::(0)     //List(0,1,2,3)


    val text = List("A,B,C","D,E,F")
    val textMapped = text.map(_.split(",").toList) // List(List("A","B","C"),List("D","E","F"))
    val textFlattened = textMapped.flatten          // List("A","B","C","D","E","F")
    val textFlatMapped = text.flatMap(_.split(",").toList) // List("A","B","C","D","E","F")

  }
}
