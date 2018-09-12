package com.wacai.stanlee.omega.input

import scala.beans.BeanProperty

/**
  * @author manshahua@wacai.com
  * @date 2017/12/19 下午5:45
  */
class UserDefinedSliceDO {
  @BeanProperty
  var min:Double = _
  @BeanProperty
  var max:Double = _
  @BeanProperty
  var expression:String = _ //加减乘除 用户自定义的算术表达式
  @BeanProperty
  var sliceNum: Int = _
  @BeanProperty
  var fieldName: String = _
  @BeanProperty
  var fieldType :String = _

  def this(min:Double, max:Double, expression:String, sliceNum:Int, fieldName:String, fieldType:String) = {
    this()
    this.min = min
    this.max = max
    this.expression = expression
    this.fieldName = fieldName
    this.sliceNum = sliceNum
    this.fieldType = fieldType
  }
}
