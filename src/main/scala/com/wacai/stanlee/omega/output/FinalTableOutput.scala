package com.wacai.stanlee.omega.output
import com.wacai.stanlee.omega.input.TableDescription
import com.wacai.stanlee.omega.output.RatioTableOutput.tableOutput
import com.wacai.stanlee.omega.util.BangJsonUtils
import org.apache.spark.sql.{DataFrameReader, SparkSession}

/**
  * @author manshahua@wacai.com
  * @date 2018/1/3 下午7:17
  */
object FinalTableOutput extends TableOutput {
  override def tableOutput(table: TableDescription, sparkSession: SparkSession): String = {
    //主键
    val keyFields:List[String] = table.getKeyFields
    val tableName:String = table.getTableName
    val partitionNum = table.getPartitionNum
    //表名
    val statisticTableName = tableName+"_"+keyFields.mkString("_") + "_statistic"//StatisticalTableOutput.tableOutput(table,sparkSession)
    val aggTableName = tableName+"_result" //AggTableOutput.tableOutput(table,sparkSession)
    val ratioTableName = tableName+"_ratio"//RatioTableOutput.tableOutput(table, sparkSession)
    val aggTableNames = 0.until(partitionNum).map(ind=>{
      aggTableName+""+(ind+1)
    })
    val ratioTableNames = 0.until(partitionNum).map(ind=>{
      ratioTableName+""+(ind+1)
    })
    val dataFrameReader:DataFrameReader = sparkSession.read
    //join条件
    val onCondition1:List[String] = keyFields.map(keyField=>{
      "a."+keyField+"="+"b."+keyField
    })
    val onCondition2:List[String] = keyFields.map(keyField=>{
      "a."+keyField+"="+"c."+keyField
    })
    val statisticColumnNames:List[String]  =  dataFrameReader.table(statisticTableName).columns.toList

    //循环生成最终结果表
    val finalTableNames = 0.until(partitionNum).map(ind=>{
      //字段名 列名
      val aggColumnNames:List[String] = dataFrameReader.table(aggTableNames(ind)).columns.toList
      val ratioColumnNames:List[String] = dataFrameReader.table(ratioTableNames(ind)).columns.toList
      //三个表join
      val statisticUsedColumnNames = if(ind==0) statisticColumnNames.filterNot(column=>keyFields.contains(column)) else Nil
      val ratioUsedColumnNames = ratioColumnNames.filterNot(column=>keyFields.contains(column))
      val finalTableName = tableName+"_final_result"+(ind+1)
      sparkSession.sql("drop table if exists "+finalTableName)
      val runSql = "create table "+finalTableName+"\nas\nselect\n\ta.*,\n"+ratioUsedColumnNames.mkString(",\n\t")+
        (if(ind==0) ",\n\t" else "") +
        statisticUsedColumnNames.mkString(",\n\t")+"\nfrom\n(\n\tselect\n\t*\n\tfrom "+aggTableNames(ind)+"\n)a\njoin\n(\n\tselect\n\t*\n\tfrom " +
        statisticTableName+"\n)b\non("+onCondition1.mkString(" and ")+")\njoin\n(\n\tselect\n\t*\n\tfrom "+ratioTableNames(ind)+"\n)c\non("+
        onCondition2.mkString(" and ")+")"
      sparkSession.sql(runSql)
      finalTableName
    })
    finalTableNames.mkString(";")
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
  }

}