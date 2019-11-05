package com.my.base.cf

import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.{SparkConf, SparkContext}


object RecommendationExample {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("RecommendationExample2").setMaster("local");
    val sc = new SparkContext(conf)

    //加载数据
    val data = sc.textFile("alstest.txt")

    val ratings = data.map(_.split(",") match { case Array(user, item, rate) =>
      Rating(user.toInt, item.toInt, rate.toDouble)
    })

    // 训练模型
    val rank = 10
    //是模型中隐语义因子的个数。
    val numIterations = 5
    val lambda = 0.01 //是ALS的正则化参数。

    val model = ALS.train(ratings, rank, numIterations, lambda)

    val usersProducts = ratings.map { case Rating(user, item, rate) =>
      (user, item)
    }

    val predictions = model.predict(usersProducts)
    // println("_----------------------------------predictions-------------------------------")
    //predictions.foreach(println)

    //这里的结果其实和上面的结果没有太多的不同，只是将评分与用户id产品id分割了下
    val predictions2 = model.predict(usersProducts).map { case Rating(user, item, rate) =>
      ((user, item), rate)
    }
    //println("------------------------------predictions2-------------------------------")
    // predictions2.foreach(println)

    //合并原始数据和评分
    val ratesAndPreds = ratings.map { case Rating(user, item, rate) =>
      ((user, item), rate)
    }.join(predictions2) //注意要是同类的才能合并，这里就不能直接合并predictions

    // println("=========================ratesAndPreds=========================")
    // ratesAndPreds foreach(println)

    //然后计算均方差，注意这里没有调用 math.sqrt方法
    val MSE = ratesAndPreds.map { case ((user, item), (r1, r2)) =>
      val err = r1 - r2
      err * err
    }.mean()
    //打印出方差结果
    println("Mean Squared Error = " + MSE)

    //获得用户的id
    val users = data.map(_.split(",") match {
      case Array(user, item, rate) => (user)
    }).distinct().collect()

    //循环用户，将id传给模型
    users.foreach(
      user => {
        val rs = model.recommendProducts(user.toInt, numIterations)
        //通过模型对用户进行商品的评分推送
        var value = ""
        var key = 0

        rs.foreach(
          r => {
            key = r.user
            value = value + r.product + ":" + r.rating + ","
          })
        println(key.toString + "   " + value)

      })

    /*val rs=model.recommendProductsForUsers(numIterations)
    rs.foreach(r=>

      r._2.foreach(x=>
        print(x.toString)
      )

      //println(r._1+":"+r._2.toString)
    )*/
  }
}