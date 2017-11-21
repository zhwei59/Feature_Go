package com.jfbank.bigdata.feature

import java.io.File

import com.moandjiezana.toml.Toml
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature.{Bucketizer, OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions.udf
import org.apache.spark.mllib.linalg.Vector

/**
  * Created by zhwei on 17/11/20.
  */
object FeatureEngin {
  def main(args: Array[String]) {
/*    val path = args(0)
              val path =try {
             getClass.getResource("/etl.toml").getPath
            }catch {
              case  e:Exception=>{e.getMessage}
            }

    val f = new File(path)
    val toml = new Toml().read(f)
    val algo=toml.getTable("ETL[0]")
    val pconf= algo.getTables("task")*/


    val conf = new SparkConf()
    val sc = new SparkContext()
    val sqlContext = new HiveContext(sc)
    val splits = Array(Double.NegativeInfinity, -0.5, 0.0, 0.5, Double.PositiveInfinity)

    val data = Array(-0.5, -0.3, 0.0, 0.2)
    val dataFrame = sqlContext.createDataFrame(data.map(Tuple1.apply)).toDF("features")

    val bucketizer = new Bucketizer()
      .setInputCol("features")
      .setOutputCol("newFeatures")
      .setSplits(splits)

    // Transform original data into its bucket index.
    val bucketedData = bucketizer.transform(dataFrame)
    bucketedData.show()

/*  val p= hiveContext.read.format("json").load("/qualitymarket/featuretest")
    p.printSchema()
      p.show()
      */

/*    import org.apache.spark.sql.functions._

    val vecToSeq = udf((v: Vector) =>v.toDense.toArray.mkString(","))
    import hiveContext.implicits._
      output.select('label,vecToSeq('features).as("features"))
        .write.format("json").save("/qualitymarket/featuretest")*/




  }
}
