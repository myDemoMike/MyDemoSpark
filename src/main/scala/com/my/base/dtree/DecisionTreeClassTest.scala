package com.my.base.dtree

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, RFormula, StringIndexer, VectorIndexer}
import org.apache.spark.sql.SparkSession


object DecisionTreeClassTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Decison Tree Classification").enableHiveSupport().getOrCreate()
    val df1 = spark.sql("")

    val df = df1.selectExpr("userid","salary","gender","education","workedyears","cast(age as int) as age","cast(label as double)")
      .dropDuplicates().limit(1000)

    val labelIndexer = new StringIndexer()
        .setInputCol("label")
        .setOutputCol("indexLabel")
        .fit(df)

    val rformula = new RFormula()
      .setFormula("label ~ ")
      .setFeaturesCol("features").setLabelCol("label")

    val rformula_model = rformula.fit(df)

    val df_ohDone = rformula_model.transform(df).select("features","indexLabel")


    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)
      .fit(df)


    val Array(trainingData, testData) = df_ohDone.randomSplit(Array(0.7, 0.3))

    val dt = new DecisionTreeClassifier().setLabelCol("label").setFeaturesCol("features")

    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    val pipeline = new Pipeline().setStages(Array(dt, labelConverter))

    val model = pipeline.fit(trainingData)

    val predictions = model.transform(testData)

    predictions.select("predictedLabel", "label", "features").show(5)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println("Test Error = " + (1.0 - accuracy))

    val treeModel = model.stages(2).asInstanceOf[DecisionTreeClassificationModel]
    println(treeModel.toString())
    println("Learned classification tree model:\n" + treeModel.toDebugString)
  }

}
