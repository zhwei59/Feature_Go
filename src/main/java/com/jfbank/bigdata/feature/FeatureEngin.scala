package com.jfbank.bigdata.feature

import java.io.File

import com.moandjiezana.toml.Toml
import org.apache.spark.ml.Transformer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature.{Bucketizer, OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.param.Param
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions.udf
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.{DataFrame, SaveMode}

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
    val hiveContext = new HiveContext(sc)
    val df = hiveContext.sql("select *,cast(category_click_count as double) as category_click_count_d,cast(category_order_count as double) as category_order_count_d from wkshop.user_raw")

    val indexer = new StringIndexer()
      .setInputCol("sex")
      .setOutputCol("sexIndex")
      .fit(df)
    val indexed = indexer.transform(df)

    val encoder = new OneHotEncoder()
      .setInputCol("sexIndex")
      .setOutputCol("sexVec")
      .setDropLast(false)
    val encoded = encoder.transform(indexed)

    val ipIndexer=new StringIndexer()
      .setInputCol("ip_area")
      .setOutputCol("ip_index")
        .fit(encoded)
    val ipidexed=ipIndexer.transform(encoded)
    val ip_area=
     encoder
      .setInputCol("ip_index")
      .setOutputCol("ip_areaVec")
      .setDropLast(false).transform(ipidexed)
   // = ipencoder
    val clientIndex=new StringIndexer()
      .setInputCol("client_id")
        .setOutputCol("clientIndex")
      .fit(ip_area)
    val clientindex=clientIndex.transform(ip_area)
    val client=encoder
      .setInputCol("clientIndex")
      .setOutputCol("clientVec")
      .setDropLast(false).transform(clientindex)
    val splits = Array(Double.NegativeInfinity,  0.0, 1,5,10,15,20,30, Double.PositiveInfinity)

    val bucketizer = new Bucketizer()
      .setInputCol("category_click_count_d")
      .setOutputCol("buke_category_click_count")
      .setSplits(splits)

    val click_count=bucketizer.transform(client)
    val order_splits = Array(Double.NegativeInfinity,  0.0, 1,3,5,7,9, Double.PositiveInfinity)
    val order_bucketizer = new Bucketizer()
      .setInputCol("category_order_count_d")
      .setOutputCol("buke_category_order_count")
      .setSplits(order_splits)
    val order_count=order_bucketizer.transform(click_count)


    import org.apache.spark.sql.functions._

    val perToDense = udf((v: String) =>Vectors.dense(v.split(",").map(_.toDouble)))
    val perToSpera335 = udf((v:String)=>Vectors.sparse(334,v.split(",").map(_.split("\\:")(0).toDouble.toInt),v.split(",").map(_.split("\\:")(1)
      .toDouble)))

    val perToSpera10=udf((v:String)=>Vectors.sparse(12,v.split(",").map(_.split("\\:")(0).toDouble.toInt),v.split(",").map(_.split("\\:")(1).toDouble)))
    import hiveContext.implicits._
    val allf=order_count.select('id,'sexVec,'ip_areaVec,'clientVec,'buke_category_click_count,'buke_category_order_count,perToDense('category_click_per).as("category_click_per")
      ,perToDense('category_order_per).as("category_order_per"),perToSpera335('category_click_rate).as("category_click_rate"),perToSpera10('category_order_rate).as("category_order_rate"))



    val assembler = new VectorAssembler ()
      .setInputCols(Array("sexVec","ip_areaVec","clientVec","buke_category_click_count","buke_category_order_count","category_click_per","category_order_per","category_click_rate","category_order_rate"))
      .setOutputCol("features")

    assembler.transform(allf).select('id,'features).write.mode(SaveMode.Overwrite).format("json").save("/qualitymarket/featureuser")

/*  val p= hiveContext.read.format("json").load("/qualitymarket/featuretest")
    p.printSchema()
      p.show()
      */






  }



}
