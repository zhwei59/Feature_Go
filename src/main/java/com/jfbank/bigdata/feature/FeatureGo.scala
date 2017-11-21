package com.jfbank.bigdata.feature

/**
  * Created by zhwei on 17/11/9.
  */

import java.io.File

import com.jfbank.bigdata.operator.{CaculatorOpertor, DimensionOperator, StatOperator}
import com.moandjiezana.toml.Toml
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * Created by zhwei on 17/11/2.
  * 所有的col_name
  * =>json
  *
  *
  */
object FeatureGo {


  def main(args: Array[String]) {
    val saveDT = DateTime.now().minusDays(1).toString("yyyy-MM-dd")
    /*
        val l="p_1,p_2,P3".split(",").toSeq
        val map=Map[String,String]("p_1"->"2","p_3"->"a:1,b:0.3")
        val g=new GenJsonTemplate(l)
        System.out.print(g.fill_cols(map))*/

    val path = args(0)
    /*    val path =try {
     getClass.getResource("/jfcanal.toml").getPath
    }catch {
      case  e:Exception=>{e.getMessage}
    }*/

    val f = new File(path)
    val cs = mutable.Set[String]().empty
    Source.fromFile(f).getLines().foreach(line => {
      if (line.contains("column") || line.contains("dimension")) {

        cs.add(line.split("\"")(1))
      }
    })

    val toml = new Toml().read(f)
    val db = toml.getString("Group.db")

    val colums = asScalaBuffer(toml.getList[String]("Group.column")).toList

    val (group, schema) = process_group(toml.getString("Group.db"), toml.getString("Group.table"), colums)


    val groupTable = process_range(toml.getString("Range.column"), toml.getString("Range.end"), schema, toml.getDouble("Range.factor"), saveDT)
    val gsql = s"select ${cs.mkString(",")} $groupTable "




    val conf = new SparkConf()
    val sc = new SparkContext()
    val hiveContext = new HiveContext(sc)

    val allColsSchema=hiveContext.sql(gsql).schema
    var allColsMap:Map[String,DataType]= Map()
    for (sf <- allColsSchema){
      allColsMap+=(sf.name->sf.dataType)
    }
    val keyFiled=colums.mkString(",")
    val broMap:Broadcast[Map[String,DataType]]=hiveContext.sparkContext.broadcast(allColsMap)
    val groupdf = hiveContext.sql(gsql)
      .map(row => (row.get(row.fieldIndex(keyFiled)).toString, row))
      .aggregateByKey(ArrayBuffer.empty[Row])(_ :+ _, _ ++ _)


    val calcs = asScalaBuffer(toml.getTables("Calculator")).toList
    calcs.foreach(t => processDistrobution(broMap,keyFiled, schema, groupdf, t, hiveContext))
    sc.stop()

    /*    import   org.apache.spark.sql.functions._
        val exprs =Map("click*f"->"sum")

        groupdf.select(expr("userid"),expr("category"),expr("click*f as click_f")).groupBy("category","userid").agg(sum("click_f").as("f_sum"))
          .groupBy("userid")
          .agg(expr("sum(f_sum) as click_sum")),expr("collect_list(f_sum) as cleect"))
            .show(20)*/


    //  select userid, sum(category_click_count) category_click, collect_list(round(category_click_count/sum(category_click_count),2))   category_click_rate from  (select userid,category,sum(click*f) category_click_count  from group_table gt group by userid,category ) r group by userid;


  }

  def process_group(db: String, table: String, column: List[String]): (String, String) = {
    (s" group by ${column.mkString(",")}", s" `${db}`.`${table} `")
  }

  def process_range(column: String, range: String, schame: String, factor: Double, day: String): String = {
    if (factor < 1)
      s" ,pow(${factor},datediff('${day}',${column})) f from ${schame} where ${column} > '${range}'"
    else s"from ${schame} where ${column} > '${range}'"
  }

  def processDistrobution(broMap:Broadcast[Map[String,DataType]],target: String, schema: String, rdd: RDD[(String, ArrayBuffer[Row])], toml: Toml, sqlContext: HiveContext) = {

    val dimension = toml.getString("dimension")
    val dtype=broMap.value.getOrElse(dimension,StringType)
    val dop = new DimensionOperator(dtype,dimension, rdd)
    val dimrdd = if (toml.contains("split")) {
      val sps = getSplit(schema, toml.getString("split").toDouble, dimension, sqlContext)
      dop.spiltDim(sps)
    }
    else {
      dop.transform()
    }
    var mao: Map[String, String] = Map()
    var stat: Map[String, String] = Map()

    val calcs = asScalaBuffer(toml.getTables("task")).toList
    for (t <- calcs) {
      mao += (t.getString("name") -> t.getString("column"))
      stat += (t.getString("name") -> t.getString("stat"))
    }

    val copt = new CaculatorOpertor(broMap,mao)
    val calcRdd = copt.sumCols(dimrdd)
    val stop = new StatOperator(stat)
    val result = stop.stat(target, calcRdd)
    val newSchema = StructType(
      StructField(target, StringType) +: stat.keys.toSeq.sorted.map(k => StructField(k, StringType)).toArray
    )
    sqlContext.createDataFrame(result, newSchema).write.mode(SaveMode.Overwrite).format("json").save("/qualitymarket/feature/"+target+"/"+dimension)

  }
  def getSplit(schema: String, split: Double, dim: String, sqlContext: HiveContext): Map[Int, Double] = {
    val maxMinSql = s"select max($dim) max_v, min($dim) min_v   from ${schema}"
   val arr= sqlContext.sql(maxMinSql).collect().toSeq.map(row => (row.getDouble(0), row.getDouble(1)))
    val (max, min)=arr(0)

    val step = (max - min) / 10.0
    val start = min
    val sp = for (i <- min to max by step) yield i
    sp.map(_.formatted("%.2f").toDouble).zipWithIndex.toMap.map(kv => (kv._2, kv._1))
  }
}

