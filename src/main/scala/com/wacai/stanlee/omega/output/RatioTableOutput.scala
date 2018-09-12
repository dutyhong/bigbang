package com.wacai.stanlee.omega.output
import java.io.{File, PrintWriter}

import com.wacai.stanlee.omega.input.TableDescription
import com.wacai.stanlee.omega.util.BangJsonUtils
import org.apache.spark.sql.{DataFrameReader, Dataset, Row, SparkSession}

/**
  * @author manshahua@wacai.com
  * @date 2018/1/3 下午3:42
  */
object RatioTableOutput extends TableOutput {
  override def tableOutput(table: TableDescription, sparkSession: SparkSession): String = {
    //先取出统计表和聚合表的各个字段名，根据字段名来做ratio特征变量的衍生
    val tableName: String = table.getTableName
    val keyFields: List[String]  = table.getKeyFields
    val partitionNum:Int = table.getPartitionNum
    val statisticTableName:String = tableName+"_"+keyFields.mkString("_")+"_statistic"
    val aggTableName:String = tableName+"_result"
    val dataFrameReader:DataFrameReader = sparkSession.read

    val ratioTableName:String = tableName+"_ratio"
    val keyTmpSql = keyFields.map(keyField=>{
      "a."+keyField
    })
    val onConditionSql = keyFields.map(keyField=>{
      "a."+keyField+"=b."+keyField
    })
    val aggTableNames = 0.until(partitionNum).map(ind=>{
      aggTableName+""+(ind+1)
    })
    val ratioTableNames = 0.until(partitionNum).map(ind=>{
      ratioTableName+""+(ind+1)
    })
    val writer = new PrintWriter(new File("ratio_table_output_sql"))

    val generatedRatioTableSql = 0.until(partitionNum).map(ind=>{
      val aggTableData:Dataset[Row] = dataFrameReader.table(aggTableNames(ind))
      val aggColumnNames:List[String] = aggTableData.columns.toList
      //取出聚合的methods
      val aggMethods:Map[String,List[String]] = table.getFieldAggMethods
      val keySets:List[String] = aggMethods.keySet.toList
      val ratioFieldsSql = keySets.flatMap(key=>{
        var tmpSql:List[String] = List()
        val sqls = aggColumnNames.map(column=>{
          //取出字段名的aggmethod 和统计字段与aggmethods进行比较
          val aggMethod:String = column.split("_")(0)
          val isAggField:Boolean = column.endsWith(key)
          if(isAggField)
            tmpSql = tmpSql:+"if( "+aggMethod+"_"+key+"=0, 0,"+(column + "/"+aggMethod+"_"+key+")")+" as "+column+"_ratio"
        })
        tmpSql
      })
      val sql1 = "drop table if exists "+ ratioTableNames(ind)
      val sql2 = "create table "+ratioTableNames(ind)+"\nas\nselect\n"+keyTmpSql.mkString(",")+",\n"+ratioFieldsSql.mkString(",\n")+
        "\nfrom \n(\nselect * from "+statisticTableName+" \n)a \njoin \n (\nselect * from "+aggTableNames(ind)+"\n)b\n on("+onConditionSql.mkString(" and ")+
        ")"
      sparkSession.sql(sql1)
      sparkSession.sql(sql2)
      sql1+";\n"+sql2
    })
    writer.write(generatedRatioTableSql.mkString("\n"))
    writer.close()
    ratioTableNames.mkString(";")
  }

  def main(args: Array[String]): Unit = {
    val table:TableDescription = new TableDescription
    val bangJsonUtils = BangJsonUtils.fromFile("tableDescription.json")
    table.init(bangJsonUtils)
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