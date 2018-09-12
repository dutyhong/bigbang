package com.wacai.stanlee.omega.output

import com.wacai.stanlee.omega.bang.StringCombination
import com.wacai.stanlee.omega.input.TableDescription
import org.apache.spark.sql.SparkSession

/**
  * @author manshahua@wacai.com
  * @date 2017/12/17 上午10:46
  */
object StatisticalTableOutput  extends TableOutput {
  override def tableOutput(input:TableDescription, spark:SparkSession):String={
    val keyFields = input.getKeyFields
    val tableName = input.getTableName
    val fieldAggMethods = input.getFieldAggMethods
    //主键有多个时，组合主键
    val keyFieldsCombination = StringCombination.combinate(keyFields)

    val sql=keyFieldsCombination.map(oneCombination=>{
      var finalSql:String = ""
      val dropTableSql = "drop table if exists " + tableName + "_"+oneCombination.mkString("_") + "_statistic"
      val tmpSql = "create table " + tableName + "_"+oneCombination.mkString("_") + "_statistic \n as \n select \n" + oneCombination.mkString(",\n") + ","
      var differentKeyAggSql:List[String] = List()
      for((k,v)<-fieldAggMethods) {
        val fieldSql = v.map(method => {
          val methodTrans = method match {
            case "c" => "count ("
            case "cd" => "count (distinct "
            case "sum" => "sum ("
            case "avg" => "avg ("
            case "max" => "max ("
            case "min" => "min ("
            case _ => throw new Exception("error method \n")
          }
          methodTrans + k + ") as " + method + "_" + k
        })
        differentKeyAggSql = differentKeyAggSql:+fieldSql.mkString(",\n")
      }
      finalSql = differentKeyAggSql.mkString(",\n")
      (dropTableSql, tmpSql+finalSql+"\n from " + input.getTableName+ "\n group by " + oneCombination.mkString(","))
      })
      sql.map(x=>{
        println(x._1+"\n"+x._2)
        spark.sql(x._1)
        spark.sql(x._2)
        x._2
      })
    tableName+"_"+keyFields.mkString("_")+"_statistic"
    }

  def main(args: Array[String]): Unit = {
    val table:TableDescription = new TableDescription
    table.init(null)
    val spark: SparkSession = SparkSession
      .builder()
      .appName("spark test")
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate
    val statisticalTableSql = tableOutput(table,spark)
    println(statisticalTableSql)
    println("ddddd")
  }
}

