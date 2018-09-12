package com.wacai.stanlee.omega.input

import scala.beans.BeanProperty
/**
  * @author manshahua@wacai.com
  * @date 2017/12/13 下午7:20
  */


class NumericSliceDO {

  //    最小值
  @BeanProperty
  var min: Double = _

  //    最大值
  @BeanProperty
  var max: Double = _

  //    切分成几段
  @BeanProperty
  var sliceNum: Int = _

  //    切分的字段名
  @BeanProperty
  var fieldName: String = _

  //    切分的字段类型
  @BeanProperty
  var fieldType: String = _

  def this(min: Double,
           max: Double,
           sliceNum: Int,
           fieldName: String,
           fieldType: String) = {
    this()
    this.min = min
    this.max = max
    this.sliceNum = sliceNum
    this.fieldName = fieldName
    this.fieldType = fieldType
  }

}



