package com.wacai.stanlee.omega.sample

import com.wacai.stanlee.launcher.BigBangMain
import org.apache.spark.sql.SparkSession

import scala.beans.BeanProperty

/**
  * @author manshahua@wacai.com
  * @date 2018/1/30 下午2:48
  */
class TableSample {
  @BeanProperty
  var tableName:String = _
  //对应着不同的采样方法：按原始表的存储大小抽样（百分比或者直接输入存储大小），按记录数行数抽样，
  @BeanProperty
  var sampleType:String = _
  @BeanProperty
  var rowsNum:Int = _
  @BeanProperty
  var memoryPercent:Double = _
  @BeanProperty
  var memorySize:String = _

  def sample(sparkSession: SparkSession):String = {
    val sampleTableName = tableName+"_sample"
    if("percent".equals(sampleType.toLowerCase)){
//      sparkSession.sql("select * from " +tableName+"\ntablesample("+memoryPercent+" percent)").write.mode("overwrite").saveAsTable(sampleTableName)
      val filePath:String = BigBangMain.filePath+sampleTableName
      sparkSession.sql("select * from " +tableName+"\ntablesample("+memoryPercent+" percent)").write.mode("overwrite").save(filePath)
      println(sampleTableName+"写入完成！！！！！")
      println("sample finished !!!!!")
    }else if("rows".equals(sampleType.toLowerCase)){
      sparkSession.sql("select * from "+tableName+"\ntablesample("+rowsNum+" rows)").write.mode("overwrite").saveAsTable(sampleTableName)
    }else{
      sparkSession.sql("select * from "+tableName+"\ntablesample("+memorySize+")").write.mode("overwrite").saveAsTable(sampleTableName)
    }
    sampleTableName
  }
}
