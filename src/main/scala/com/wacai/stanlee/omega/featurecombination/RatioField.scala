package com.wacai.stanlee.omega.featurecombination

import scala.beans.BeanProperty

/**
  * @author manshahua@wacai.com
  * @date 2018/1/20 下午4:05
  */
class RatioField {
  @BeanProperty
  var numerator:String = _
  @BeanProperty
  var denominator:String = _
//  @BeanProperty
//  var combinateType:String = _
  def this(numerator:String, denominator:String)={
    this()
    this.numerator = numerator
  this.denominator = denominator
  }
}
