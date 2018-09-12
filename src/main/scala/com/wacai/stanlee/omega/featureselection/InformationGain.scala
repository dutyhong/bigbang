package com.wacai.stanlee.omega.featureselection

import org.apache.spark.sql.{Dataset, Row, functions}

import scala.beans.BeanProperty
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * @author manshahua@wacai.com
  * @date 2018/1/29 下午3:08
  */
object InformationGain {
//  //类别数
//  @BeanProperty
//  var categoryNum:Int = _
//  //样本数
//  @BeanProperty
//  var sampleNum:Int = _
//  //某一属性对应区间内的样本数
//  @BeanProperty
//  var attributeSampleNums:List[Int] = List()
  def getSingleFeatureSample(featureSelectorParam: FeatureSelectorParam): Double ={
    var samples = featureSelectorParam.getSamples
    val labelName = featureSelectorParam.getLabelName
    samples = samples.withColumn(labelName, functions.col(labelName).cast("long"))
    val featureColumnNames = featureSelectorParam.getFeatureColumnNames
    val length = featureColumnNames.length
    val singleFeatureSample = samples.select(featureColumnNames(0), labelName)
    val intervalValues:List[Double]  = List(0,2,5,8,20,10000)
    val conditionEntropy = computeIntervalEntropy(singleFeatureSample, featureColumnNames(0), labelName, intervalValues)
    conditionEntropy
  }
  def computeIntervalEntropy(singleFeatureSample:Dataset[Row], featureName:String, labelName:String, intervalValues:List[Double]):Double = {
    val rows:java.util.List[Row] = singleFeatureSample.collectAsList()
    val size:Int = rows.size()
    val intervalSize:Int = intervalValues.size
    //存放每个区间对应的样本数
    val intervalNums:mutable.ArrayBuffer[Int] = mutable.ArrayBuffer()
    //存放每个label对应的样本数
    val labelNums:mutable.Map[String, Long] = mutable.Map()
    0.until(intervalSize).foreach(intervalInd=>{
      val beforeValue:Double = intervalValues(intervalInd)
      val afterValue:Double = intervalValues(intervalInd+1)
      0.until(size).foreach(ind=>{
        val row:Row = rows.get(ind)
        val tmpValue = row.getAs(featureName)
        val featureValue = if(tmpValue.isInstanceOf[Double]) tmpValue else tmpValue.asInstanceOf[Double]//        match {
//          case d: Double => d
//          case _ => row.getAs(featureName).asInstanceOf[Double]
//        }
        //统计每个区间的样本个数
        if(featureValue>=beforeValue && featureValue<afterValue)
          intervalNums(intervalInd) = intervalNums(intervalInd)+1

        val labelValue:String = if(row.getAs(labelName).isInstanceOf[String]) row.getAs(labelName) else row.getAs(labelName).asInstanceOf[String]
//        match {
//          case d: String =>d
//          case _=> row.getAs(labelName).asInstanceOf[String]
//        }
        //统计label得个数放入map
        if(labelNums.contains(labelValue)){
          var value = labelNums(labelValue)
          value = value + 1
          labelNums+=(labelValue->value)
        }else{
          labelNums+=(labelValue->1l)
        }
      })
    })
    //统计每个区间内属于各个label的样本数
    val labelNames:collection.Set[String] = labelNums.keySet
    var intervalCategoryMap: List[mutable.Map[String, Int]] = List[mutable.Map[String, Int]]()
    intervalCategoryMap = 0.until(intervalSize).map(intervalInd=> {
      val beforeValue: Double = intervalValues(intervalInd)
      val afterValue: Double = intervalValues(intervalInd + 1)
      val labelMap:mutable.Map[String, Int] = mutable.Map()
      0.until(size).foreach(ind=> {
        val row: Row = rows.get(ind)
        val tmpValue = row.getAs(featureName)
        val featureValue = if(tmpValue.isInstanceOf[Double]) tmpValue else tmpValue.asInstanceOf[Double]

        //统计每个区间的样本个数
        if (featureValue >= beforeValue && featureValue < afterValue){
          val labelValue:String = if(row.getAs(labelName).isInstanceOf[String]) row.getAs(labelName) else row.getAs(labelName).asInstanceOf[String]

          if(labelMap.contains(labelValue)){
            var value:Int = labelMap(labelValue)
            value = value + 1
            labelMap(labelValue) = value
          }else{
            labelMap(labelValue) = 1
          }
        }
      })
      labelMap
    }).toList

    //开始计算条件熵 -Da/D(Da0/Da*log(Da0/Da)+Da1/Da*log(Da1/Da))
    val intervalCnt = 0
    var conditionEntropy:Double = 0d
    intervalNums.foreach(intervalNum=>{
      val categoryProb:Double = intervalNum/size.asInstanceOf[Double]
      val labelMap:mutable.Map[String, Int] = intervalCategoryMap(intervalCnt)
      var categoryEntropy:Double = 0d
      labelMap.foreach(x=>{
        val key:String = x._1
        val value:Int = x._2
        val tmp:Double = value/intervalNum.asInstanceOf[Double]
        categoryEntropy = categoryEntropy + tmp*Math.log(tmp)
      })
      conditionEntropy = conditionEntropy + categoryProb*categoryEntropy
    })
    conditionEntropy
  }
}
