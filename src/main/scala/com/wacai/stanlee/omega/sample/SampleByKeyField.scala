package com.wacai.stanlee.omega.sample

import com.wacai.stanlee.omega.input.TableDescription
import org.apache.spark.sql.SparkSession

/**
  * @author manshahua@wacai.com
  * @date 2018/1/29 下午5:40
  */
class SampleByKeyField {
  def sampleByKey(tableDescription: TableDescription, sparkSession: SparkSession): Unit ={
    val tableName:String = tableDescription.getTableName
    val keyField:String = tableDescription.getKeyFields.mkString(",")

  }
}
