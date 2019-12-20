package com.my.base.fpgrowth

import org.apache.spark.SparkConf
import org.apache.spark.ml.fpm.FPGrowth
import org.apache.spark.sql.SparkSession


/**
  * @Author: Yuan Liu
  * @Description:
  * @Date: Created in 14:51 2019/11/14
  *
  *        Good Good Study Day Day Up
  */
object FPGrowthDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().config(new SparkConf().setMaster("local[*]")).getOrCreate()

    import spark.implicits._
    val dataset = spark.createDataset(Seq(
      "1 2 5",
      "1 2 3 5",
      "1 2",
    "1")).rdd.map(t => t.split(" ")).toDF("items").filter("size(items) > 1")
    dataset.show()
    //
    // minConfidence：生成关联规则的最小置信度。 置信度表明关联规则
    // 例如，如果在交易项目集X中出现4次，X和Y仅出现2次，则规则X => Y的置信度则为2/4 = 0.5。
    // 该参数不会影响频繁项集的挖掘，但会指定从频繁项集生成关联规则的最小置信度。
    /**
      * minSupport：对项目集进行频繁识别的最低支持  数据集有3行。3*0.5  意思是至少要出现1.5次  即为2次
      *
      * minConfidence：生成关联规则的最小置信度。 置信度表明关联规则经常被发现的频率。
      * 例如，如果在交易项目集X中出现4次，X和Y仅出现2次，则规则X => Y的置信度则为2/4 = 0.5。
      * 该参数不会影响频繁项集的挖掘，但会指定从频繁项集生成关联规则的最小置信度。
      *
      */
    val fpgrowth = new FPGrowth().setItemsCol("items").setMinSupport(0.5).setMinConfidence(0.6)
    val model = fpgrowth.fit(dataset)

    // Display frequent itemsets.
    model.freqItemsets.show()

    // Display generated association rules.
    model.associationRules.show()
    //
    //    // transform examines the input items against all the association rules and summarize the
    //    // consequents as prediction
    model.transform(dataset).show()
    model.transform(dataset).filter("size(prediction) > 0").show()
  }
}