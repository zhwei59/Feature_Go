package com.jfbank.bigdata.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.collection.immutable
import scala.collection.mutable.{ArrayBuffer, Map}

/**
  * Created by zhwei on 17/11/7.
  */
class DimensionOperator(dataType: DataType,dim:String,rdd:RDD[(String,ArrayBuffer[Row])])extends Serializable {

  def spiltDim(mao:immutable.Map[Int,Double]): RDD[(String,Map[String,ArrayBuffer[Row]])] ={
    rdd.map{
      case (person,rows)=>{
        val A =Map[String,ArrayBuffer[Row]]()
        for (i <- 0 until  mao.keys.size){
          val start=mao.getOrElse(i,-1.0)
          val end=mao.getOrElse(i+1,-1.0)
          if(end>0) {
            val arr = rows.filter(row => {

              val d = dataType match {
                case DoubleType => row.getDouble(row.fieldIndex(dim))
                case LongType =>row.getLong(row.fieldIndex(dim)).toDouble
                case IntegerType =>row.getInt(row.fieldIndex(dim)).toDouble
                case StringType =>row.getString(row.fieldIndex(dim)).toDouble
                case _=>0.0
              }
              d > start && d <= end
            })
            if(arr.size>0)
              A+=(i.toString->arr)
          }
        }
        (person,A)
      }
    }


  }

  def transform():RDD[(String,Map[String,ArrayBuffer[Row]])]={

    rdd.map{
      case (person,rows)=>{
        val A:Map[String,ArrayBuffer[Row]] = Map()
        for (row <-rows){
          val d = dataType match {
            case DoubleType => row.getDouble(row.fieldIndex(dim)).toString
            case LongType =>row.getLong(row.fieldIndex(dim)).toString
            case IntegerType =>row.getInt(row.fieldIndex(dim)).toString
            case StringType =>row.getString(row.fieldIndex(dim))
            case _=>"0.0"
          }

          val arr=A.getOrElse(d,new ArrayBuffer[Row])
          arr.append(row)
          A +=(d->arr)

        }
        (person,A)
      }
    }
  }
}
