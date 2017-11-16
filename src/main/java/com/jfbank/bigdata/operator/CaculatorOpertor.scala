package com.jfbank.bigdata.operator

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by zhwei on 17/11/8.
  */
class CaculatorOpertor(dtypeM:Broadcast[Map[String,DataType]],mao:Map[String,String]) extends Serializable{


  def sumCols(rdd:RDD[(String,scala.collection.mutable.Map[String,ArrayBuffer[Row]])]): RDD[(String,Map[String,Row])] ={
    val newSchema = StructType(
      mao.keys.map(k=>StructField(k,DoubleType)).toArray
    )
    rdd.map{
      case (person,dataMap)=>{

        var B:Map[String,Row]=Map()
        for(kv<-dataMap){
          var A:Map[String,Double]=Map()
          for (row<- kv._2){
            for (makv <- mao){
              val dtype=dtypeM.value.getOrElse(makv._2,StringType)
              val rd=dtype
                 match {
                  case DoubleType=> row.getDouble (row.fieldIndex (makv._2))
                  case LongType=> row.getLong(row.fieldIndex (makv._2)).toDouble
                  case IntegerType =>row.getInt(row.fieldIndex (makv._2)).toDouble
                  case StringType =>row.getString(row.fieldIndex (makv._2)).toDouble
                }
              val value=A.getOrElse(makv._2,0.0)
              val r=rd+value
              A += (makv._1->r)
            }
          }
          val newRow = new GenericRowWithSchema(A.values.toArray,newSchema)
          B+=(kv._1->newRow)
        }
        (person,B)
      }
    }


  }

}
