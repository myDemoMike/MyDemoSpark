package com.my.base.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.Rating

object MovieLensALS {
  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("MovieLensALS")
      .set("spark.executor.memory", "1g")

    val sc = new SparkContext(conf)


    var rdd1 = sc.makeRDD(1 to 10,2)

    val ratings = sc.textFile("/u1.test").map {
      line => val fields = line.split("\t")
        (fields(3).toLong % 10, Rating(fields(0) toInt, fields(1).toInt, fields(2).toDouble))
    }

    val movies = sc.textFile("/u.item").map {
      line => val fields = line.split('|')
        (fields(0).toInt, fields(1))
    }

    val numRatings = ratings.count
    val numUsers = ratings.map(_._2.user).distinct.count
    val numMovies = ratings.map(_._2.product).distinct.count
    println("Got " + numRatings + " ratings from "
      + numUsers + " users on " + numMovies + " movies.")
  }
}



