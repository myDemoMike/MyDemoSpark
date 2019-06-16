package com.my.base.agg

/**
  * Created by 89819 on 2018/3/4.
  */
object AggregateTest {
  def main(args: Array[String]) {
    val data = List(2,5,8,1,2,6,9,4,3,5)
    val res = data.par.aggregate((0,0))(
      // seqOp
      (acc, number) => (acc._1+number, acc._2+1),
      // combOp
      (par1, par2) => (par1._1+par2._1, par1._2+par2._2)
    )
    println(res)
  }
}
