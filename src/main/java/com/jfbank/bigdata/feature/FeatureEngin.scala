package com.jfbank.bigdata.feature

import java.io.File

import com.moandjiezana.toml.Toml

/**
  * Created by zhwei on 17/11/20.
  */
object FeatureEngin {
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
  }
}
