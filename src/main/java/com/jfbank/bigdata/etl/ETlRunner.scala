package com.jfbank.bigdata.etl

import java.io.File

import scala.collection.JavaConversions._
import com.moandjiezana.toml.Toml
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
/**
  * Created by zhwei on 17/11/20.
  *
  */
object ETlRunner {
  def main(args: Array[String]) {
       val path = args(0)
/*           val path =try {
         getClass.getResource("/etl.toml").getPath
        }catch {
          case  e:Exception=>{e.getMessage}
        }*/

    val f = new File(path)
    val toml = new Toml().read(f)
    val algo=toml.getTable("ETL[0]")
    val pconf= algo.getTables("task")

    val conf = new SparkConf()
    val sc = new SparkContext()
    val hiveContext = new HiveContext(sc)
    pconf.foreach(t=>processToml(t,hiveContext))

    sc.stop()
  }

  def processToml(toml:Toml,hiveContext: HiveContext): Unit ={
    val dist=toml.getString("output")
    val drop=toml.getBoolean("drop")
    val sql=toml.getString("sql")
    if(drop){
      hiveContext.sql("drop table if exists "+dist)
    }

    hiveContext.sql("create table "+dist+" as "+sql)
  }

}
