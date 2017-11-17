package com.jfbank.bigdata.models
import java.util
import java.util.concurrent.atomic.AtomicReference
import scala.collection.JavaConversions._
/**
  * Created by zhwei on 17/11/16.
  */
abstract class  BaseModelRuner[T] {

var configParams:util.List[util.Map[Any, Any]]= _
def config[T](name:String,_configParams:util.List[util.Map[Any, Any]]):Option[T]={
config(0,name,_configParams)

}
  def config[T](index:Int,name:String,_configParams:util.List[util.Map[Any, Any]]):Option[T]={
    if(_configParams.size()>0 && _configParams(0).contains(name)) {
      Some(_configParams(index).get(name).asInstanceOf[T])

    }
    else None
  }
def init(pmap:util.List[util.Map[Any, Any]])={
  configParams=pmap
}
  val instance = new AtomicReference[Any]()


}
