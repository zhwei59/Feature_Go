package com.jfbank.bigdata.feature

import java.io.File

import com.jfbank.bigdata.models.{BaseEstimator, BaseModelRuner}
import java.util

import com.moandjiezana.toml.Toml
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.tuning.TrainValidationSplitModel
import org.apache.spark.sql.hive.HiveContext
import org.joda.time.DateTime

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * Created by zhwei on 17/11/16.
  */
object RunModel extends BaseModelRuner{
  val mapping: Map[String, String] = Map("lr"->"com.jfbank.bigdata.models.als.LREstimator")

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

    //val path = args(0)
        val path =try {
     getClass.getResource("/lr.toml").getPath
    }catch {
      case  e:Exception=>{e.getMessage}
    }

    val f = new File(path)

    val cs = mutable.Set[String]().empty
/*    Source.fromFile(f).getLines().foreach(line => {
      if (line.contains("column") || line.contains("dimension")) {

        cs.add(line.split("\"")(1))
      }
    })*/

    val toml = new Toml().read(f)
    val algo=toml.getTable("Algo[0]")
    val pconf= algo.getTable("Params[0]")
    val confMap = new ArrayBuffer[Map[String, Any]]()

    confMap.append(Map("name"->algo.getString("name")))
    confMap.append(Map("path"->algo.getString("path")))
    confMap.append(Map("outPutTable"->algo.getString("outPutTable")))
    confMap.append(Map("regParam"->pconf.getString("regParam")
      ,"elasticNetParam"->pconf.getString("elasticNetParam")
      ,"maxIter"->pconf.getString("maxIter")
    ))


    val input=hiveContext.sql("select * from  wkshop.lr_data_test")
    val bae = algorithm(
      input,
      confMap).
      asInstanceOf[BaseEstimator]
    val model = bae.fit

    model match {
      case a: TrainValidationSplitModel =>
        a.bestModel.getClass.getMethod("save", classOf[String]).invoke(a.bestModel, path)
      case _ => model.getClass.getMethod("save", classOf[String]).invoke(model, path)
    }
  }

//  def result(processors: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {
//
//    if (!params.containsKey(FUNC)) {
//
//      try {
//        val _inputTableName = inputTableName.get
//
//        val input = sqlContextHolder(params).table(_inputTableName)
//
//        val newParams = _configParams.map(f => f.map(k => (k._1.asInstanceOf[String], k._2)).toMap).toArray
//        val bae = algorithm(
//          input,
//          newParams).
//          asInstanceOf[BaseEstimator]
//        val model = bae.fit
//
//        model match {
//          case a: TrainValidationSplitModel =>
//            a.bestModel.getClass.getMethod("save", classOf[String]).invoke(a.bestModel, path)
//          case _ => model.getClass.getMethod("save", classOf[String]).invoke(model, path)
//        }
//
//
//      } catch {
//        case e: Exception => e.printStackTrace()
//      }
//
//
//    } else {
//      val oldDf = middleResult.get(0).asInstanceOf[DataFrame]
//      val func = params.get("_func_").asInstanceOf[(DataFrame) => DataFrame]
//
//      try {
//        val df = func(oldDf)
//        val newParams = _configParams.map(f => f.map(k => (k._1.asInstanceOf[String], k._2)).toMap).toArray
//        val bae = algorithm(
//          df,
//          newParams).
//          asInstanceOf[BaseAlgorithmEstimator]
//        val model = bae.fit
//        model.getClass.getMethod("save", classOf[String]).invoke(model, path)
//      } catch {
//        case e: Exception => e.printStackTrace()
//      }
//
//
//      params.remove("sql")
//
//    }
//    return if (middleResult == null) List() else middleResult
//
//
//  }

}
