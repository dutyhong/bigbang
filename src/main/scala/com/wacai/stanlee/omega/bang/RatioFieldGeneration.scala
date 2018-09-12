package com.wacai.stanlee.omega.bang

import com.wacai.stanlee.omega.featurecombination.RatioField

import scala.beans.BeanProperty
import scala.collection.mutable

/**
  * @author manshahua@wacai.com
  * @date 2018/1/20 下午3:08
  */
object RatioFieldGeneration {
//  @BeanProperty
//  var ratioFields:List[RatioField] = List()
  def generate(fieldNameSlices:mutable.Map[String, BangField]):List[RatioField]={
    val fields = fieldNameSlices.keySet.toList
    val binaryCombinations:List[List[String]] = StringCombination.binaryCombinate(fields)
    var ratioFields:List[RatioField] = List()
    binaryCombinations.foreach(binaryCombination=>{
      //将每个组合取出来然后比较大小，用大的除以小的 c_use_time_1_start_time_1_start_time/c_start_time_1_start_time
      if(binaryCombination.size!=2)
      {
        throw new RuntimeException("字段生成不对")
      }
      val firstField:String = binaryCombination(0)
      val secondField:String = binaryCombination(1)
      val firstBangField:BangField = fieldNameSlices(firstField)
      val secondBangField:BangField = fieldNameSlices(secondField)
      val firstAggMethod:String = firstBangField.getAggMethod
      val secondAggMethod:String = secondBangField.getAggMethod
      val firstAggField:String = firstBangField.getAggField
      val secondAggField:String = secondBangField.getAggField
      val firstOriginalFields = firstBangField.getOriginalFields
      val secondOriginalFields = secondBangField.getOriginalFields
      if(firstAggMethod.equals(secondAggMethod)&&firstAggField.equals(secondAggField)){
        if(list1ContainsList2(firstOriginalFields,secondOriginalFields)&&(firstOriginalFields.size-secondOriginalFields.size==1)){
          val numerator:String = firstAggMethod+"_"+firstOriginalFields.mkString("_")+"_"+firstAggField
          val denominator:String = secondAggMethod+"_"+secondOriginalFields.mkString("_")+"_"+secondAggField
          val ratioField:RatioField = new RatioField(numerator, denominator)
          ratioFields = ratioFields:+ratioField
        }else if(list1ContainsList2(secondOriginalFields,firstOriginalFields)&&(secondOriginalFields.size-firstOriginalFields.size==1)){
          val numerator:String = firstAggMethod+"_"+firstOriginalFields.mkString("_")+"_"+firstAggField
          val denominator:String = secondAggMethod+"_"+secondOriginalFields.mkString("_")+"_"+secondAggField
          val ratioField:RatioField = new RatioField(denominator, numerator)
          ratioFields = ratioFields:+ratioField
        }else{

        }
      }
    })
    ratioFields
  }
  def list1ContainsList2(list1: List[String],list2: List[String]): Boolean =
  {
    list2.foreach(list=>{
      if(!list1.contains(list))
        return false
    })
    true
  }
  def main(args: Array[String]): Unit = {
    val list1 = List("start_time_1", "sss", "use_time_1")
    val list2 = List("start_time_1","use_time_1","ddd")
    println(list1ContainsList2(list1,list2))
    val bangField1:BangField = new BangField("c","start_time",List("use_time_1","start_time_1"))
    val bangField2:BangField = new BangField("c","start_time",List("use_time_1"))
    val bangField3:BangField = new BangField("c","start_time",List("start_time_1"))
    val fieldNameSlices:mutable.Map[String, BangField] = mutable.Map("c_use_time_1_start_time_1_start_time"->bangField1,"c_use_time_1_start_time"->bangField2,
    "c_start_time_1_start_time"->bangField3)
    val ratioFields:List[RatioField] = generate(fieldNameSlices)
    val ratioFieldss = ratioFields.map(ratioField=>{
      ratioField.getNumerator+":"+ratioField.getDenominator
    })
    println(ratioFieldss.mkString("\n"))
  }

}
