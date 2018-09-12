package com.wacai.stanlee.omega

import breeze.linalg.DenseVector
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

/**
  * @author manshahua@wacai.com
  * @date 2017/12/14 下午7:30
  */
object Test {
  def main(args: Array[String]): Unit = {
    val str: List[String] = List("age1", "age2")
    println(str.mkString(" and "))
    val testMap = Map("a"->"111", "b"->"222")
    val cc = for((k,v)<-testMap)
      k+"ddd"
    println(cc)
    val xxx = "count"
    val yyy = if(xxx=="count distinct") "count ( distinct" else (xxx+" ) ")
    println(yyy)
    for(j<-0 until(5))
      println(j)
    val sparkSession:SparkSession = SparkSession.builder()
      .appName("test")
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()
    val structFiled1:StructField =  StructField("age", DoubleType, true)
    val structField2:StructField =  StructField("id", StringType, true)
    val structType: StructType = StructType(List(structFiled1, structField2))
    val rowRDD:java.util.List[Row] = new java.util.ArrayList()
    rowRDD.add(Row(1.0,"23"))
    rowRDD.add(Row(2.0,"24"))

    val dataSet:Dataset[Row] = sparkSession.createDataFrame(rowRDD, structType)
    dataSet.show()
    val tmp:breeze.linalg.DenseVector[Double] = DenseVector(1,2,3,4,5)
    val ttt:Array[Int] = Array(1,2,3)
    ttt.toSet
  }
}
