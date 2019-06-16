package com.my.base.scala

/**
  * Created by yuan on 2017/12/20.
  */
object Test_for {
  def main(args: Array[String]) {

    //1到10
    for (i <- 1 to 10) {
      print(i + "  ")
    }
    println("----------------")
    //1到9
    for (i <- 1 until 10) {
      print(i + "--")
    }
    println("----------------")
    for (a <- 1 to 3; b <- 5 to 6) {
      print(a, b + "  ")
    }
    println("----------------")
    //集合
    var numlist = List(1, 2, 3, 4, 7)

    for (i <- numlist) {
      print(i + "--")
    }

    //数组
    var numarray = Array(1, 2, 3, 4, 5)

    val len = numarray.length


    print(len)
    //三维数组
    //var matrix_array = ofDim[Int](3,3)

  }

}
