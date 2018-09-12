package com.wacai.stanlee.omega.input

import scala.beans.BeanProperty



/**
  * @author manshahua@wacai.com
  * @date 2017/12/13 下午7:22
  */
class DateSliceDO {

  //    时间切分类型：月  month 天 day， 年 year
  @BeanProperty
  var sliceType: String = _

  //    时间切分间隔，n月n天n年
  @BeanProperty
  var sliceTypeInterval: Int = _
  @BeanProperty
  var sliceNum: Int = _
  //每次计算时间切片时的结束时间
  @BeanProperty
  var endTime: String = _
  @BeanProperty
  var fieldName: String = _

  @BeanProperty
  var fieldType: String = _

  def this(sliceType: String,
           sliceTypeInterval: Int,
           sliceNum: Int,
           fieldName: String,
           fieldType: String,
           endTime: String) = {
    this()
    this.sliceType = fieldType
    this.fieldName = fieldName
    this.sliceType = sliceType
    this.sliceTypeInterval = sliceTypeInterval
    this.endTime = endTime
    this.sliceNum = sliceNum
  }

}


