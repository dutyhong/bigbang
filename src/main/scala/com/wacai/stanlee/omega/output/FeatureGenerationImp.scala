package com.wacai.stanlee.omega.output

import java.io.{File, PrintWriter, Writer}

import com.wacai.stanlee.omega.featurecombination.FeatureCombinationTableOutput
import com.wacai.stanlee.omega.input.TableDescription
import org.apache.spark.sql.SparkSession

import scala.beans.BeanProperty
import scala.collection.immutable.Map

/**
  * @author manshahua@wacai.com
  * @date 2018/1/23 下午5:22
  */
class FeatureGenerationImp {
  @BeanProperty
  var fieldGenerationSqls:Map[String, String] = Map()
  def featureGenerationImp(sparkSession: SparkSession, tableDescription: TableDescription): List[String] = { //        String statisticTableName = StatisticalTableOutput.tableOutput(tableDescription, sparkSession);
    val aggTableOutput = new AggTableOutput
    val aggTableName = aggTableOutput.tableOutput(tableDescription, sparkSession)
    val featureCombinationTableOutput = new FeatureCombinationTableOutput
    val finalTableName = featureCombinationTableOutput.tableOutput(aggTableOutput, sparkSession, tableDescription)
    println("最后的特征表名为："+finalTableName)
    val aggFieldGenerationSql = aggTableOutput.getFieldGenerationSqls
    val ratioFieldGenerationSql = featureCombinationTableOutput.getFieldGenerationSqls
    val writer:Writer = new PrintWriter(new File("field_generation_sql"))
    aggFieldGenerationSql.foreach(sql=>{
      writer.write(sql._1+": "+sql._2+"\n")
      fieldGenerationSqls+=(sql._1->sql._2)
    })
    ratioFieldGenerationSql.foreach(sql=>{
      writer.write(sql._1+": "+sql._2+"\n")
      fieldGenerationSqls+=(sql._1->sql._2)
    })
    writer.close()
    List(finalTableName)
    //
  }
}
