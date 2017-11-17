package com.jfbank.bigdata.models.als

import com.jfbank.bigdata.models.BaseEstimator
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, Evaluator}
import org.apache.spark.ml.tuning.TrainValidationSplit
import org.apache.spark.sql.DataFrame

/**
  * Created by zhwei on 17/11/16.
  */
class LREstimator(training: DataFrame, params: Array[Map[String, Any]]) extends  BaseEstimator {
  val lr = new LogisticRegression()

  override def fit: Model[_] = {
    val paramGrid = mlParams(params.tail)
    val vectorSize = if (params.head.contains("dicTable")) {
      training.sqlContext.table(params.head.getOrElse("dicTable", "").toString).count()
    } else 0l
    if (params.length <= 1) {
      lr.fit(source(training, vectorSize.toInt), paramGrid(0))
    } else {
      val trainValidationSplit = new TrainValidationSplit()
        .setEstimator(lr)
        .setEvaluator(evaluator)
        .setEstimatorParamMaps(paramGrid)
        .setTrainRatio(0.8)
      trainValidationSplit.fit(source(training, vectorSize.toInt))
    }

  }

  override def algorithm: Estimator[_] = lr

  override def evaluator: Evaluator = new BinaryClassificationEvaluator()
}
