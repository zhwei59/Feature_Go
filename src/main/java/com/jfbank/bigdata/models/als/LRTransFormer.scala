package com.jfbank.bigdata.models.als

import com.jfbank.bigdata.models.BaseTransformer
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.sql.DataFrame

/**
  * Created by zhwei on 17/11/16.
  */
class LRTransFormer(path:String) extends BaseTransformer{
  val lr=LogisticRegressionModel.load(path)
  override def transform(data: DataFrame): DataFrame = lr.transform(data)
}
