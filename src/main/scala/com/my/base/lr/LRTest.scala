package com.my.base.lr

import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression}
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.sql.SparkSession


object LRTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
    val spark = SparkSession.builder.appName("LRTest").config(conf).enableHiveSupport().getOrCreate()

    // 订单id 产品id 加入购物车的顺序 是否再次购买过   行为数据
    val priors = spark.sql("select * from test.priors")
    //   行为数据
    val trains = spark.sql("select * from test.trains ")
    val products = spark.sql("select * from test.products")
//    val aisles = spark.sql("select * from test.aisles")
    val orders = spark.sql("select * from test.orders")
//    val departments = spark.sql("select * from test.departments")

    val priorsOrders = priors.join(orders,"order_id")

    val product3Feat = FeatureEngineer.Product3Feat(priors,products)
    product3Feat.show(20,false)
//      .drop(" product_name")
    val userFeat = FeatureEngineer.UserFeat(orders,priorsOrders)
    val userXproductFeat = FeatureEngineer.userXproduct(priorsOrders)

    println("split orders : train, test")
    val train_order = orders.filter("eval_set='train'")
    val test_order = orders.filter("eval_set='test'")

    val df1 = FeatureEngineer.features(selectOrder = train_order,
                              userFeat,
                              product3Feat,
                              userXproductFeat,trains,labels_given = true)


    val df = df1.limit(1000)

    val rformula = new RFormula()
      .setFormula("label ~ user_mean_days + user_nb_orders+user_total_items + user_total_unique_items +user_average_basket"+
    "+order_hour_of_day+days_since_prior_order+days_since_ratio+aisle_id+department_id+pro_cnt+sum_reord+reord_rate"+
    "+up_orders+sum_pos_in_cart+up_order_ratio+up_average_pos_in_cart+up_orders_since_last")
      .setFeaturesCol("features").setLabelCol("label")

    val df_ohDone = rformula
      .fit(df)
      .transform(df)
      .select("features","label").cache()

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0)
//      .setElasticNetParam(0)

    val Array(trainingData, testData) = df_ohDone.randomSplit(Array(0.7, 0.3))
    val lrModel = lr.fit(trainingData)

    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    val trainingSummary = lrModel.summary

    // Obtain the objective per iteration.
    val objectiveHistory = trainingSummary.objectiveHistory
    objectiveHistory.foreach(loss => println(loss))

    // Obtain the metrics useful to judge performance on test data.
    // We cast the summary to a BinaryLogisticRegressionSummary since the problem is a
    // binary classification problem.
    val binarySummary = trainingSummary.asInstanceOf[BinaryLogisticRegressionSummary]

    // Obtain the receiver-operating characteristic as a dataframe and areaUnderROC.
    val roc = binarySummary.roc
    roc.show()
    println(binarySummary.areaUnderROC)

    // Set the model threshold to maximize F-Measure
    val fMeasure = binarySummary.fMeasureByThreshold
   // val maxFMeasure = fMeasure.selectExpr(max("F-Measure")).head().getDouble(0)
   // val bestThreshold = fMeasure.filter(fMeasure("F-Measure")===maxFMeasure)
    //  .select("threshold").head().getDouble(0)
   // lrModel.setThreshold(bestThreshold)
  }

}
