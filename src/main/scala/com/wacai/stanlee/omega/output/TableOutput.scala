package com.wacai.stanlee.omega.output

import com.wacai.stanlee.omega.input.TableDescription
import org.apache.spark.sql.SparkSession

/**
  * @author manshahua@wacai.com
  * @date 2017/12/18 上午11:30
  */
trait TableOutput {
  def tableOutput(table:TableDescription, spark:SparkSession):String

}
