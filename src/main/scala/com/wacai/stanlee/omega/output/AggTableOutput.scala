package com.wacai.stanlee.omega.output

import java.io.{File, PrintWriter}

import com.wacai.stanlee.launcher.BigBangMain
import com.wacai.stanlee.omega.bang.{BangField, SqlMaker, StringCombination}
import com.wacai.stanlee.omega.input.TableDescription
import com.wacai.stanlee.omega.util.BangJsonUtils
import org.apache.spark.sql._

import scala.beans.BeanProperty
import scala.collection.mutable
import scala.util.Random

/**
  * @author manshahua@wacai.com
  * @date 2017/12/18 上午11:28
  */
class AggTableOutput extends TableOutput {
  //字段对应的名字和切分名
  @BeanProperty
  var fieldNameSlices:mutable.Map[String, BangField] = mutable.Map()
  @BeanProperty
  var aggTableName:String = _
  @BeanProperty
  var fieldGenerationSqls:Map[String, String] = Map()
  override def tableOutput(table:TableDescription, spark:SparkSession): String ={
    //表名
    var tableName = table.getTableName
    val labelName = table.getLabelName
    val partitionNum = table.getPartitionNum
    //获取主key
    val keyFields = table.getKeyFields
    //map （field->List(filed1, filed2, field3)）
    val newFileds = table.getNewFields
    //list case when xxx then filed i
    val numericSqlString = table.getNumericSqlString
    val categoricalSqlString = table.getCategoricalSqlString
    val dateSqlString = table.getDateSqlString
    val userDefinedSqlString = table.getUserDefinedSqlString
    //总的sql
    val totalSqlString = (numericSqlString ++ categoricalSqlString ++ dateSqlString ++ userDefinedSqlString).mkString(",\n")
    var keys:List[String] = List()
    newFileds.foreach{case (m, n)=>
      val key:String = m
      keys = keys:+key
    }
    //输出所有的需要切片的key的组合
    val fieldCombinations:List[List[String]] = StringCombination.combinate(keys)
    //输出所有的主key的组合
    val keyFieldCombinations = StringCombination.combinate(keyFields)
    //对于对不同字段可以选择的组合
    val fieldList = fieldCombinations.flatMap(x=>{
      val listList = x.map(y=>{
        newFileds(y)
      })
      StringCombination.crossJoin(listList)
    })

    println(newFileds)
    println(fieldCombinations)
    println(fieldList)
    //将生成的sql语句写入文件便于查看
    val writer = new PrintWriter(new File("agg_table_output_sql"))
    val tmpTableName = tableName + "_tmp"
//    spark.sql("drop table if exists " + tmpTableName)

    //支持分布式
    val tableSql:String = if(null==totalSqlString||"".equals(totalSqlString)) "select * from "+tableName else "select *, \n" + totalSqlString + " from "+ tableName
    val tmpSql = "create table " + tmpTableName +"\n" +
      "as \n"+
      tableSql
    //    spark.sql(tmpSql)
    println("中间表生成的sql："+tmpSql)
    spark.sql(tableSql).write.mode("overwrite").save(BigBangMain.filePath+tmpTableName)
    spark.read.load(BigBangMain.filePath+tmpTableName).createOrReplaceTempView(tmpTableName)
    println("临时表写入完成！！！！！！")
    spark.sql("select * from " + tmpTableName).show(10)
//    spark.sql("select * from " + tmpTableName).write.mode("overwrite").save(BigBangMain.filePath+tmpTableName)
    writer.write("中间临时表产生sql：\n"+tmpSql)
//    spark.sql("select * from "+tmpTableName+ " limit 10").show()
    //可以根据主键生成对不同主键的group by，一般只需要一个就可以
    //    keyFieldCombinations.map(x=>{
    //      val resultTableName = tableName +"_"+ x.mkString("_")+"_result"
    //      spark.sql("drop table if exists " + resultTableName)
    //      val savedSql = "create table "+ resultTableName+" \n as \n " +
    //        "select \n " +
    //        x.mkString(",\n") +", \n " + groupBySql + " from "+tmpTableName +
    //        " group by " + x.mkString(",")
    //      writer.write(savedSql)
    //      spark.sql(savedSql+"\n")
    //      spark.sql("select * from " + resultTableName +" limit 10").show()
    //    })
    //定义一个Map里面放置key为需要计算的字段名，value为需要计算使用的方法
    val calcMethodMap:Map[String, List[String]] = table.getFieldAggMethods //Map("brand_id"->List("c", "cd"), "item_id"->List("c", "cd"))
    val sqlMaker:SqlMaker = new SqlMaker
    var finalSqlString = sqlMaker.makeCoreSql(calcMethodMap, fieldList)
    //得到的是（c_start_time_1_start_time, (c, start_time, list(start_time_1)）
    fieldNameSlices = sqlMaker.getFieldNameSlices
    //得到每个字段对应的sql代码
    fieldGenerationSqls = sqlMaker.getFieldGenerationSql
    //把fieldNameSlices里面所有的字段取出来进行组合

    //打乱这个字段列表 不让cd都在同一个表里计算
    finalSqlString = Random.shuffle(finalSqlString)
    //将一个大表分成几个小表来跑 这样可以更快
    val length:Int = finalSqlString.size
    val times:Int = length/partitionNum
    val partitionNumNew = if (length%partitionNum==0) partitionNum else partitionNum+1
    table.setPartitionNum(partitionNumNew)
    val left:Int = length%partitionNum
    var finalSqlStrings:List[List[String]] = List()
    var tmpList: List[String] = List()

    0.until(length).foreach(ind=>{
      if(ind%times==0&&ind!=0){
        finalSqlStrings = finalSqlStrings:+tmpList
        tmpList = Nil
      }
      tmpList = tmpList:+finalSqlString(ind)
    })
    finalSqlStrings = finalSqlStrings:+tmpList
    //    println(conditions)
    var index: Int = 0
//    val resultTableNames = finalSqlStrings.map(sqls=>{
//      val groupBySql = sqls.mkString(",\n")
//      val x = keyFields
//      index = index + 1
//      //    val resultTableName = tableName +"_"+ x.mkString("_")+"_result"
//      val resultTableName = tableName +"_result"+index
//      spark.sql("drop table if exists " + resultTableName)
//      val tmpLabelStr = if(null==labelName) ", \n" else ","+labelName+", \n "
//      val tmpLabelGroupStr = if(null==labelName) "" else ", "+labelName
//      val savedSql = "create table "+ resultTableName+" \n as \n " +
//        "select \n " +
//        x.mkString(",\n") + tmpLabelStr + groupBySql + " from "+tmpTableName +
//        " group by " + x.mkString(",")+tmpLabelGroupStr
//      writer.write(savedSql+"\n")
//      spark.sql(savedSql)
//      println("第"+resultTableName+"完成！！")
////      spark.sql("select * from " + resultTableName +" limit 10").show()
//      resultTableName
//    })
//    resultTableNames.mkString(";")

    //定义多个dataset最终将所有数据放到一个表里
    val dataSetTables:List[Dataset[Row]] = finalSqlStrings.map(sqls=> {
      System.out.print("一个表完成")
      val groupBySql = sqls.mkString(",\n")
      val x = keyFields
//      index = index + 1
      //    val resultTableName = tableName +"_"+ x.mkString("_")+"_result"
//      val resultTableName = tableName + "_result" + index
      val tmpLabelStr = if (null == labelName) ", \n" else "," + labelName + ", \n "
      val tmpLabelGroupStr = if (null == labelName) "" else ", " + labelName
//      val savedSql = "create table " + resultTableName + " \n as \n " +
      val tmpSmallSql = "select \n " +
        x.mkString(",\n") + tmpLabelStr + groupBySql + " from " + tmpTableName +
        " group by " + x.mkString(",") + tmpLabelGroupStr
      writer.write("\n"+tmpSmallSql+"\n")
//      spark.sql(tmpSmallSql).show(1)
        spark.sql(tmpSmallSql)
    })
    val allColumnNames = dataSetTables.flatMap(dataset=>{
      val columnNames = dataset.columns
      columnNames.filterNot(x=>keyFields.contains(x)||x.equals(labelName))
      })
    val joinIds:Seq[String] = keyFields:+labelName
    var tmpDataset = dataSetTables(0)
    val size:Int = dataSetTables.size
    1.until(size).foreach(ind=>{
      System.out.println("第"+ind+"个表join完成")
      tmpDataset = tmpDataset.join(dataSetTables(ind),joinIds)
    })
    val resultTableName = tableName+"_result"
    tmpDataset.repartition(20).write.mode("overwrite").parquet(BigBangMain.filePath+resultTableName)
    println("agg table 写入 finished !!!!!!!")
    spark.read.load(BigBangMain.filePath+resultTableName).createOrReplaceTempView(resultTableName)

    //    dataFrameWriter.mode("overwrite").saveAsTable(resultTableName)
    writer.close()
    aggTableName = resultTableName
    println("聚合表名为："+resultTableName)
    resultTableName
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