package com.my.base.als

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession

/**
  *
  * 交替最小二乘法
  */
object ALSTrain {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ALS Train").enableHiveSupport().getOrCreate()
    val df = spark.sql("select user_id,item_id,rating,cast(`timestamp1` as long) from badou.udata limit 1000")
    val SP = 890582400
////    通过随机的方式进行数据集的划分
//    val Array(training,test) = df.randomSplit(Array(0.8,0.2))

    //    因为这里的userid需要是NumericType，所以需要对这个userid进行编码
    val indexer_user = new StringIndexer().setInputCol("user_id").setOutputCol("user_id_indexed")
    val df_user_encode = indexer_user.fit(df).transform(df)
    //    同样需要对info做一个编码
    val indexer_info = new StringIndexer().setInputCol("item_id").setOutputCol("item_id_indexed")
    val df_encode = indexer_info.fit(df_user_encode).transform(df_user_encode)
    // 存map（user_id_map,item_id_map）
    val user_id_map = df_encode.select("user_id_indexed","user_id").rdd.map(x=>(x(0).toString,x(1).toString)).collectAsMap()

    val item_id_map = df_encode.select("item_id_indexed","item_id").rdd.map(x=>(x(0).toString,x(1).toString)).collectAsMap()

    //  通过时间划分数据集
    val training = df_encode.filter(s"`timestamp1`<$SP").select("item_id_indexed","user_id_indexed","rating").persist()
    val test = df_encode.filter(s"`timestamp`>=$SP").select("item_id_indexed","user_id_indexed","rating").persist()

    // setRank 隐变量向量  setRegParam 正则
    val als = new ALS().setMaxIter(10).setRank(10).setRegParam(0.01).setUserCol("user_id_indexed").setItemCol("item_id_indexed").setRatingCol("rating").setPredictionCol("prediction")
      // 设置setImplicitPrefs为true更好
      .setImplicitPrefs(true)
    val model = als.fit(training)
    training.unpersist()
    model.userFactors.show()
    model.itemFactors.show()

    val predictions = model.transform(test)
    test.unpersist()

    // 评价
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")

    val rmse = evaluator.evaluate(predictions)

    println(s"Root-mean-square error = $rmse")

  }

}
