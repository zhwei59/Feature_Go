package com.jfbank.bigdata.models
import java.util
import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.sql.DataFrame

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
/**
  * Created by zhwei on 17/11/16.
  */
abstract class  BaseModelRuner {

var _configParams:util.List[util.Map[String, Any]]= _
def config(name:String,_configParams:util.List[util.Map[String, Any]]):Option[String]={
config(0,name,_configParams)

}
  def config[T](index:Int,name:String,_configParams:util.List[util.Map[String, Any]]):Option[String]={
    if(_configParams.size()>0 && _configParams(0).contains(name)) {
      Some(_configParams(index).get(name).asInstanceOf[String])

    }
    else None
  }
def init(pmap:util.List[util.Map[String, Any]])={
  _configParams=pmap
}
  val instance = new AtomicReference[Any]()

  val mapping: Map[String, String] = Map("lr"->"com.jfbank.bigdata.models.als.LREstimator",
    "lrt"->"com.jfbank.bigdata.models.als.LRTransFormer"
  )
  def algorithm(training: DataFrame, params: util.List[util.Map[String, Any]]) = {
    val clzzName = mapping(config("name", _configParams).get)
    if (instance.get() == null) {
      instance.compareAndSet(null, Class.forName(clzzName).
        getConstructors.head.
        newInstance(training, params))
    }
    instance.get()
  }

  def algorithm(path: String) = {
    val name = config("transname", _configParams).get
    val clzzName = mapping.getOrElse(name, name)
    if (instance.get() == null) {
      instance.compareAndSet(null, Class.forName(clzzName).
        getConstructors.head.
        newInstance(path))
    }
    instance.get()
  }

  def parameters = {
    import scala.collection.JavaConversions._
    (_configParams(0) - "path" - "name" - "outPutTable").map { f =>
      (f._1.toString, f._2.toString)
    }.toMap
  }

}
