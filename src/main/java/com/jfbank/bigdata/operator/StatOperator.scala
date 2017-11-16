package com.jfbank.bigdata.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import scala.collection.mutable
/**
  * Created by zhwei on 17/11/8.
  */
class StatOperator(mao:Map[String,String] ) extends Serializable{



  def stat(uid:String,rdd:RDD[(String,Map[String,Row])]): RDD[Row] ={
    val sorted=mao.keys.toSeq.sorted
    val newSchema = StructType(
      StructField(uid,StringType) +: sorted.map(k=>StructField(k,StringType)).toArray
    )
    val filedsOrder=sorted.zipWithIndex.toMap
    rdd.map{
      case (person,row)=>{
        val A=mutable.Map[String,String]()
        for(kv<-mao){
          kv._2 match {
            case "sum"=>{
              var value=0.0
              for (dkv<-row.values){
                value+=dkv.getDouble(dkv.fieldIndex(kv._1))
              }
              A+=(kv._1->value.toString)
            }
            case "rate"=>{
              var sum=0.0
              for (dkv<-row.values){
                sum+=dkv.getDouble(dkv.fieldIndex(kv._1))
              }
              if (sum==0) sum+=1
              val rmap=row.mapValues(dkv=> {val ra = dkv.getDouble(dkv.fieldIndex(kv._1)) / sum
                ra.formatted("%.4f")})
              val rarry=  for (rp<-rmap) yield rp._1+":"+rp._2
              A+=(kv._1->rarry.mkString(","))
            }
            case "mean"=>{
              var sum=0.0
              val len=row.values.size

              for (dkv<-row.values){
                sum+=dkv.getDouble(dkv.fieldIndex(kv._1))
              }
              val mean=(sum/len).formatted("%.4f").toString
              A+=(kv._1->mean)

            }
            case "percentile"=>{
              val arr=row.values.toSeq.map(dkv=>dkv.getDouble(dkv.fieldIndex(kv._1))).sorted
              val length=arr.size
              var percent=1
              val sb=new StringBuilder()
              val pre=Seq(0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0)
              for (p <- pre) {
                val k = math.ceil((arr.length - 1) * (p / 100.0)).toInt
                sb.append(p.toString)
                sb.append(":")
                sb.append(arr(k).toString)
                sb.append(",")
              }
             val percenttalil= sb.toString().stripSuffix(",")
              A+=(kv._1->percenttalil)
            }
          }

        }
        val oriarr=A.values.toArray
        for (kv<-filedsOrder.keys){
          oriarr(filedsOrder.getOrElse(kv,-1))=A.getOrElse(kv,"404")
        }
        new GenericRowWithSchema(person +:oriarr,newSchema)
      }


    }

  }


}
