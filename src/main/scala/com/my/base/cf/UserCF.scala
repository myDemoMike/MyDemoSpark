package com.my.base.cf

import breeze.numerics.{pow, sqrt}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object UserCF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("User Base CF").master("local[4]")
      .enableHiveSupport().getOrCreate()
    val df = spark.sql("select user_id,item_id,rating from badou.udata limit 1000")
    df.show(10)
    val df_t = spark.sql("select user_id as user_id1,item_id as item_id1,rating as rating1 from badou.udata limit 1000")
    val df_user_sim = User_sim(df, df_t, spark)
    val df_user_item = simUserItem(df_user_sim, df, spark)
    df_user_item.show()
  }

  // 用户的相似度
  def User_sim(df: DataFrame, df_t: DataFrame, spark: SparkSession): DataFrame = {

    import spark.implicits._
    val userScoreSum = df.rdd.map(x => (x(0).toString, x(2).toString))
      .groupByKey().mapValues(x => sqrt(x.toArray.map(line => pow(line.toDouble, 2)).sum))
    val df_user_sum = userScoreSum.toDF("user_id_sum", "rating_sqrt_sum")

    //过滤。只选取对相同物品有过打分的数据。其余的忽略
    val df_decare = df.join(df_t, df("item_id") === df_t("item_id1")).filter("cast(user_id as long) < cast(user_id1 as long)")

    val product_udf = udf((s1: Int, s2: Int) => s1.toDouble * s2.toDouble)
    val df_product = df_decare.withColumn("rating_product", product_udf(col("rating"), col("rating1"))).select("user_id", "user_id1", "rating_product")
    val df_sim_group = df_product.groupBy("user_id", "user_id1").agg("rating_product" -> "sum").withColumnRenamed("sum(rating_product)", "rating_sum_pro")
    val df_sim_1 = df_sim_group.join(df_user_sum, df_sim_group("user_id") === df_user_sum("user_id_sum")).drop("user_id_sum")
    val df_user_sum1 = df_user_sum.withColumnRenamed("rating_sqrt_sum", "rating_sqrt_sum1")
    val df_sim = df_sim_1.join(df_user_sum1, df_product("user_id1") === df_user_sum1("user_id_sum")).drop("user_id_sum")

    val sim_udf = udf((pro: Double, s1: Double, s2: Double) => pro / (s1 * s2))
    val df_res = df_sim.withColumn("sim", sim_udf(col("rating_sum_pro"), col("rating_sqrt_sum"), col("rating_sqrt_sum1"))).select("user_id", "user_id1", "sim")
    df_res
  }


  //获取相似用户的物品集合
  def simUserItem(df_res: DataFrame, df: DataFrame, spark: SparkSession): DataFrame = {
    //2.1 取得前n个相似的用户
    import spark.implicits._
    val df_nsim = df_res.rdd.map(x => (x(0).toString, (x(1).toString, x(2).toString))).groupByKey()
      .mapValues { x => x.toArray.sortWith((x, y) => x._2 > y._2).slice(0, 10) }
      .flatMapValues(x => x).toDF("user_id", "user_id1_sim")
      .selectExpr("user_id", "user_id1_sim._1 as user_id1", "user_id1_sim._2 as sim")
    val df_user_item1 = df.rdd.map(x => (x(0).toString, x(1).toString + "_" + x(2).toString))
      .groupByKey().mapValues { x => x.toArray }.toDF("user_id_gen_item", "item_rating_array")

    val df_user_item2 = df_user_item1.withColumnRenamed("item_rating_array", "item_rating_array1")

    //2.2分别为user_id和user_id1携带items进行过滤
    val df_gen_item_tmp = df_nsim.join(df_user_item1, df_nsim("user_id") === df_user_item1("user_id_gen_item")).drop("user_id_gen_item")
    val df_gen_item = df_gen_item_tmp.join(df_user_item2, df_gen_item_tmp("user_id1") === df_user_item2("user_id_gen_item")).drop("user_id_gen_item")

    //2.3用一个udf过滤相似用户user_id1中包含user_id已经打过分的物品
    val filter_udf = udf { (items1: Seq[String], items2: Seq[String]) =>
      val fMap = items1.map { x =>
        val l = x.split("_")
        (l(0), l(1))
      }.toMap

      items2.filter { x =>
        val l = x.split("_")
        fMap.getOrElse(l(0), "-") == "-"
      }
    }
    val df_filter_item = df_gen_item.withColumn("filtered_item", filter_udf(col("item_rating_array"), col("item_rating_array1"))).select("user_id", "sim", "filtered_item")

    //2.4公式计算 相似度*rating   算出缺失值
    val sim_rating_udf = udf { (sim: Double, filter_item: Seq[String]) =>
      filter_item.map { x =>
        val l = x.split("_")
        l(0) + "_" + l(1).toDouble * sim
      }
    }
    //    case class userItemArr(user_id:String,item_product:Seq[(String,String)])
    // 有的数据为空，是已经打过分的数据
    val itemSimRating = df_filter_item
      .withColumn("item_product", sim_rating_udf(col("sim"), col("filtered_item")))
      .select("user_id", "item_product")
    val df_user_item_score = itemSimRating.select(itemSimRating("user_id"), explode(itemSimRating("item_product"))).toDF("user_id", "item_product").selectExpr("user_id", "split(item_product,'_')[0] as item_id", "cast(split(item_product,'_')[1] as double) as score")
    df_user_item_score
  }


  def UserBaseCF_RDD(data: RDD[Array[String]]): Unit = {

    // get user -> (item, score)
    val rddUserRating = data.map { arr =>
      val line = (arr(0).toString, arr(1).toString, arr.toString)
      line match {
        case (user, item, score) => (user, item, score)
      }
    }

    //  get user‘s score square sum: sqrt(s1^2 + s2^2 + ... + sn^2)
    val rddUserScoreSum = rddUserRating
      .map(fields => (fields._1, pow(fields._3.toString.toDouble, 2)))
      .reduceByKey(_ + _)
      .map(fields => (fields._1, sqrt(fields._2)))

    // get <item, (user, score)>
    val rddItemInfo = rddUserRating.map(tokens => tokens._2 -> (tokens._1.toString, tokens._3))

    // get <item, ((user1, score1), (user2, score2))>
    val rddUserPairs = rddItemInfo.join(rddItemInfo).filter { tokens =>
      tokens match {
        case (item, ((user1, score1), (user2, score2))) => user1.toDouble < user2.toDouble
      }
    }

    // get score1 * score2 and reduce by key (user1, user2) and get sum
    val rddUserPairScore = rddUserPairs.map { tokens =>
      tokens match {
        case (video, ((user1, score1), (user2, score2))) => (user1, user2) -> score1.toString.toDouble * score2.toString.toDouble
      }
    }.reduceByKey(_ + _)

    // get cos similarity
    val rddSimilarityTmp = rddUserPairScore.map {
      case ((user1, user2), productScore) => user1 -> (user2, productScore)
    }.join(rddUserScoreSum)

    val rddSimilarity = rddSimilarityTmp.map {
      tokens =>
        tokens match {
          case (user1, ((user2, productScore), squareSumScore1)) => user2 -> ((user1, squareSumScore1), productScore)
        }
    }.join(rddUserScoreSum)

    val userSimilarity = rddSimilarity.map {
      tokens =>
        tokens match {
          case (user2, (((user1, squareSumScore1), productScore), squareSumScore2)) => (user1, user2) -> productScore / (squareSumScore1 * squareSumScore2)
        }
    }

    for (i <- userSimilarity) {
      print(s"$i\n")

    }


  }

}
