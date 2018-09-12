package com.wacai.stanlee.omega.bang

import scala.beans.BeanProperty

/**
  * @author manshahua@wacai.com
  * @date 2018/1/20 下午3:22
  */
class BangField {
  @BeanProperty
  var aggMethod:String = _
  @BeanProperty
  var aggField:String = _
  @BeanProperty
  var originalFields:List[String] = List()
  def this(aggMethod:String, aggField:String, originalFields:List[String])=
  {
    this()
    this.aggMethod = aggMethod
    this.aggField = aggField
    this.originalFields = originalFields
  }
}
