package com.my.base.feature

import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.{Matrices, Vector, Vectors}

object VectorDemo {

  def main(args: Array[String]): Unit = {
    // [1.0,0.0,3.0]
    val dv: Vector = Vectors.dense(1.0, 0.0, 3.0)
    println("dv=====================")
    println(dv)
    // (3,[0,2],[1.0,3.0])   向量的表示形式。(3,[0,2],[1.0,3.0])   维度为3 第0维为1.0 第2维为3.0 其它为0
    val sv1: Vector = Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0))
    println("sv1=====================")
    println(sv1)
    val sv2: Vector = Vectors.sparse(3, Seq((0, 1.0), (2, 3.0)))
    println("sv2=====================")
    println(sv2)


   // (1.0,[1.0,0.0,3.0])
    val pos = LabeledPoint(1.0, Vectors.dense(1.0, 0.0, 3.0))
    println("pos=====================")
    println(pos)

    // (0.0,(3,[0,2],[1.0,3.0]))
    // Create a labeled point with a negative label and a sparse feature vector.
    val neg = LabeledPoint(0.0, Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0)))
    println("neg=====================")
    println(neg)


    // Create a dense matrix ((1.0, 2.0), (3.0, 4.0), (5.0, 6.0))
    val dm = Matrices.dense(3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))

    println("dm=====================")
    println(dm)
    // Create a sparse matrix ((9.0, 0.0), (0.0, 8.0), (0.0, 6.0))
    val sm = Matrices.sparse(3, 2, Array(0, 1, 3), Array(0, 2, 1), Array(9, 6, 8))

    println("sm=====================")
    println(sm)

  }
}
