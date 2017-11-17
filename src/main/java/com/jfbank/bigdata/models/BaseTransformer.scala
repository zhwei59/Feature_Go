package com.jfbank.bigdata.models

import org.apache.spark.sql.DataFrame

/**
  * Created by zhwei on 17/11/16.
  */
trait BaseTransformer {
def transform(data:DataFrame):DataFrame
}
