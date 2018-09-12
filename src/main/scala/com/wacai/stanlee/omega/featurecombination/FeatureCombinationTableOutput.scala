package com.wacai.stanlee.omega.featurecombination

import java.io.{File, PrintWriter, Writer}

import com.wacai.stanlee.launcher.BigBangMain
import com.wacai.stanlee.omega.bang.{BangField, RatioFieldGeneration}
import com.wacai.stanlee.omega.input.TableDescription
import com.wacai.stanlee.omega.output.AggTableOutput
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.beans.BeanProperty
import scala.collection.mutable

/**
  * @author manshahua@wacai.com
  * @date 2018/1/20 下午5:14
  */
 class FeatureCombinationTableOutput {
  //生成字段名对应的sql
  @BeanProperty
  var fieldGenerationSqls:Map[String, String] = Map()

  def tableOutput(aggTableOutput: AggTableOutput, sparkSession: SparkSession, tableDescription: TableDescription):String ={
    val aggTableName:String = aggTableOutput.getAggTableName
    sparkSession.read.load(BigBangMain.filePath+aggTableName).createOrReplaceTempView(aggTableName)
    val inputTableName:String = tableDescription.getTableName
    val fieldNameSlices:mutable.Map[String, BangField] = aggTableOutput.getFieldNameSlices
    val ratioFields:List[RatioField] = RatioFieldGeneration.generate(fieldNameSlices)
    val ratioStrFields = ratioFields.map(ratioField=>{
      val numerator:String = ratioField.getNumerator
      val denominator:String = ratioField.getDenominator
      val fieldName = s"ratio_$numerator"+s"_$denominator"
      val sql = s"if($denominator=0, 0, $numerator/$denominator) as $fieldName"

      fieldGenerationSqls+=(fieldName->sql)
      sql
    })
    val writer:Writer = new PrintWriter(new File("ratio_table_output_sql"))
    writer.write(ratioStrFields.mkString(",\n\t"))
    writer.close()
    val featureCombinateSql = if(null==ratioStrFields||Nil==ratioStrFields) s"select \n\t* "+s"\n from $aggTableName"
    else s"select \n\t*, \n\t"+ratioStrFields.mkString(",\n\t")+s"\n from $aggTableName"
    val featureCombinateTable:Dataset[Row] = sparkSession.sql(featureCombinateSql)
    val featureCombinationTableName = inputTableName+"_final_result"
    featureCombinateTable.createOrReplaceTempView(featureCombinationTableName)
//    featureCombinateTable.write.mode("overwrite").saveAsTable(featureCombinationTableName)
    println("保存ratio表：！！！！！数据查看：")
    featureCombinateTable.show(10)
    featureCombinateTable.repartition(20).write.mode("overwrite").parquet(BigBangMain.filePath+featureCombinationTableName)
//    featureCombinationData.show(10)
    featureCombinationTableName
  }
}
