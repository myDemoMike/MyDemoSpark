package com.my.base.lr

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object FeatureEngineer {
  //增加product的3个feature
  def Product3Feat(priors:DataFrame,products:DataFrame): DataFrame ={
    val priorsProdOrders = priors.groupBy("product_id").count()
    val priorsProdReorders = priors.selectExpr("product_id","cast(reordered as int)")
      .groupBy("product_id").sum("reordered")
      .withColumnRenamed("sum(reordered)","sum_reord")
    val priors2feat = priorsProdOrders.join(priorsProdReorders,"product_id")

    val priorsProdReordRate = priors2feat
      .selectExpr("product_id",
        "count as pro_cnt",
        "sum_reord",
        "cast(sum_reord as double)/cast(count as double) as reord_rate")
    products.join(priorsProdReordRate,"product_id")
  }

  //user feature generator
  def UserFeat(orders:DataFrame,priorsOrders:DataFrame): DataFrame ={
    val spark = orders.sparkSession
    import spark.implicits._
    val UserOrderMeanDay = orders
      .selectExpr("user_id","cast(days_since_prior_order as double)")
      .groupBy("user_id")
      .mean("days_since_prior_order")
      .withColumnRenamed("avg(days_since_prior_order)","mean_days")

    val UserOrderNumber = orders.groupBy("user_id").count()
      .withColumnRenamed("count","nb_orders")

    val UserOrderFeat = UserOrderMeanDay
      .join(UserOrderNumber,"user_id")

    val userPriorItems = priorsOrders.groupBy("user_id").count()
      .withColumnRenamed("count","total_items")

    val userPriorProductRecords = priorsOrders
      .select("user_id","product_id").rdd.map(x=>(x(0).toString,x(1).toString))
      .groupByKey().mapValues{row=>row.toSet.mkString("_")}
      .toDF("user_id","prod_records")
      .selectExpr("user_id","prod_records","size(split(prod_records,'_')) as total_unique_items")
    val userFeat = UserOrderFeat.join(userPriorItems,"user_id")
      .join(userPriorProductRecords,"user_id")

    userFeat.selectExpr("user_id",
      "mean_days",
      "nb_orders",
      "total_items",
      "prod_records",
      "total_unique_items",
      "cast(total_items as double)/cast(nb_orders as double) as average_basket")
  }
  // cross feature: number_orders, last order id, sum position in cart
  def userXproduct(priorsOrders:DataFrame): DataFrame = {
    val spark = priorsOrders.sparkSession
    import spark.implicits._

    val userXproTmp = priorsOrders
      .selectExpr("concat_ws('_',user_id,product_id) as user_prod",
        "order_number",
        "order_id",
        "cast(add_to_cart_order as int) as add_to_cart_order_int")
    val userXproNbOrd = userXproTmp
      .groupBy("user_prod")
      .agg(count("user_prod").as("orders_cnt"),
        sum("add_to_cart_order_int").as("sum_pos_in_cart"))
    //.count().withColumnRenamed("count","X_nb_orders")
    val userXproductLastOrder = userXproTmp.rdd.map(x => (x(0).toString, (x(1).toString, x(2).toString)))
      .groupByKey()
      .mapValues { iter => iter.toArray.maxBy(x => x._1.toDouble)._2 }
      .toDF("user_prod", "last_order_id")

    userXproNbOrd.join(userXproductLastOrder,"user_prod")
  }
  // combine all features in one dataFrame
  def features(selectOrder:DataFrame,
               userFeat:DataFrame,
               product3Feat:DataFrame,
               userXprod:DataFrame,
               trains:DataFrame,
               labels_given:Boolean=false): DataFrame ={
  //all user features
    println("user related features")
    val userFeatFlat = userFeat.selectExpr("user_id",
  "explode(split(prod_records,'_')) as product_id",
  "mean_days as user_mean_days",
  "nb_orders as user_nb_orders",
  "total_items as user_total_items",
  "total_unique_items as user_total_unique_items",
  "average_basket as user_average_basket")
    val orderUserFeat = selectOrder
      .join(userFeatFlat,"user_id")
      .selectExpr("product_id","order_id",
        "concat_ws('_',user_id,product_id) as user_prod",
        "user_mean_days","user_nb_orders",
        "user_total_items","user_total_unique_items",
        "user_average_basket",
        "order_hour_of_day","days_since_prior_order",
        "cast(days_since_prior_order as double)/cast(user_mean_days as double) as days_since_ratio")

    println("add product features!")
    val orderUserProd = orderUserFeat.join(product3Feat,"product_id").drop("product_name")
    println("user product cross features!")
    val orderUserXprod = orderUserProd.join(userXprod,"user_prod")
      .selectExpr("order_id","product_id","user_mean_days","user_nb_orders",
        "user_total_items","user_total_unique_items",
        "user_average_basket",
        "order_hour_of_day","days_since_prior_order",
        "days_since_ratio","aisle_id","department_id","pro_cnt",
        "sum_reord","reord_rate",
        "orders_cnt as up_orders","sum_pos_in_cart",
        "cast(orders_cnt as double)/cast(user_nb_orders as double) as up_order_ratio",
        "cast(sum_pos_in_cart as double)/cast(orders_cnt as double) as up_average_pos_in_cart",
        "cast(user_nb_orders as double)-cast(last_order_id as double) as  up_orders_since_last"
      )

    if(labels_given){
      val train_tmp = trains.selectExpr("order_id","product_id","1 as label")
      orderUserXprod.join(train_tmp,
        orderUserFeat("order_id")===train_tmp("order_id")&&orderUserFeat("product_id")===train_tmp("product_id"),"left_outer")
        .drop(train_tmp("order_id"))
        .drop(train_tmp("product_id"))
        .na.fill(0)
    }else{
      orderUserXprod
    }
  }
}
