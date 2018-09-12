package com.wacai.stanlee.launcher

import com.wacai.stanlee.omega.input.TableDescription
import com.wacai.stanlee.omega.util.BangJsonUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
  * @author manshahua@wacai.com
  * @date 2018/1/25 下午4:09
  */
object SaveDataToHdfs {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark SQL basic example")
      .enableHiveSupport()
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("WARN")
    val bangJsonUtils: BangJsonUtils = BangJsonUtils.fromFile("tableDescription.json")
    val tableDescription: TableDescription = new TableDescription
    tableDescription.init(bangJsonUtils)
    val resultTableName = tableDescription.getTableName+"_final_result"
    val resultData:Dataset[Row] = sparkSession.read.table(resultTableName)
//    val output = new Path("hdfs://10.1.169.1:9000/data/dw/raw/modeling_db.db/"+resultTableName+"/")
//    val hdfs = org.apache.hadoop.fs.FileSystem.get(
//      new java.net.URI("hdfs://10.1.169.1:9000"), new org.apache.hadoop.conf.Configuration())
//
    // 删除输出目录
//    if (hdfs.exists(output)) hdfs.delete(output, false)
//    val output = new Path("hdfs://172.16.48.191:9000/data/bigbang/"+resultTableName)
//    val hdfs = org.apache.hadoop.fs.FileSystem.get(
//      new java.net.URI("hdfs://172.16.48.191:9000"), new org.apache.hadoop.conf.Configuration())
//
    // 删除输出目录
//    if (hdfs.exists(output)) hdfs.delete(output, false)
    resultData.write.mode("overwrite").csv("hdfs://10.1.169.1:9000/data/dw/raw/modeling_db.db/"+resultTableName)
    System.out.println("数据存储完成****************")
  }
}
