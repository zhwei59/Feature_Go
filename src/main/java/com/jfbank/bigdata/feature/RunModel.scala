package com.jfbank.bigdata.feature

import java.io.File

import com.jfbank.bigdata.models.{BaseEstimator, BaseModelRuner, BaseTransformer}
import java.util

import com.moandjiezana.toml.Toml
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.tuning.TrainValidationSplitModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.hive.HiveContext
import org.joda.time.DateTime

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * Created by zhwei on 17/11/16.
  */
object RunModel extends BaseModelRuner{


  def main(args: Array[String]) {
    val conf = new SparkConf()
    val sc = new SparkContext()
    val hiveContext = new HiveContext(sc)
    val saveDT = DateTime.now().minusDays(1).toString("yyyy-MM-dd")
    /*
        val l="p_1,p_2,P3".split(",").toSeq
        val map=Map[String,String]("p_1"->"2","p_3"->"a:1,b:0.3")
        val g=new GenJsonTemplate(l)
        System.out.print(g.fill_cols(map))*/

    val path = args(0)
/*        val path =try {
     getClass.getResource("/lr.toml").getPath
    }catch {
      case  e:Exception=>{e.getMessage}
    }*/

    val f = new File(path)

/*    Source.fromFile(f).getLines().foreach(line => {
      if (line.contains("column") || line.contains("dimension")) {

        cs.add(line.split("\"")(1))
      }
    })*/

    val toml = new Toml().read(f)
    val algo=toml.getTable("Algo[0]")
    val job=args(1)
    val pconf= algo.getTables("Params")
    val modelSave=algo.getString("modelpath")
    val datapath=algo.getString("datapath")

    val confMap = new util.ArrayList[util.Map[String, Any]]()
    val tamp=new util.HashMap[String,Any]()
    tamp.put("name",algo.getString("name"))
    tamp.put("transname","lrt")
    tamp.put("path",algo.getString("datapath"))
    tamp.put("outPutTable",algo.getString("outPutTable"))
    val algomap=new util.HashMap[String,Any]()
    confMap.add(tamp)
    pconf.foreach(t=>{
      val pname=t.getString("name")
      val dt=t.getString("type")
      val v=dt match {
        case "int"=>t.getString("value").split(",").map(_.toInt)
        case "double"=>t.getString("value").split(",").map(_.toDouble)
        case _=>t.getString("value").split(",")
      }
      v.foreach(tv=>{
        val algomap=new util.HashMap[String,Any]()
        algomap.put(pname,tv)
        confMap.add(algomap)
      })

    })

    this.init(confMap)
    val lablerdd=MLUtils.loadLibSVMFile(sc,datapath)
    val input=hiveContext.createDataFrame(lablerdd).toDF("label","features")

      //hiveContext.sql("select * from  wkshop.lr_data_test")

if (job=="train"){


    val hadoopconfig=sc.hadoopConfiguration
    val hdfs=FileSystem.get(hadoopconfig)
    try {hdfs.delete(new Path(modelSave),true)}
    catch {case e:Throwable=>{}}

  val bae = algorithm(
    input,
    confMap).
    asInstanceOf[BaseEstimator]
  val model = bae.fit

  model match {
    case a: TrainValidationSplitModel =>
      a.bestModel.getClass.getMethod("save", classOf[String]).invoke(a.bestModel, modelSave)
    case _ => model.getClass.getMethod("save", classOf[String]).invoke(model, modelSave)
  }
}
    else
    {
      val output = algorithm(modelSave).asInstanceOf[BaseTransformer].transform(input)
      output.show(100)
    }

  }





}
