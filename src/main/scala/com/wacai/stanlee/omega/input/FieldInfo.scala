package com.wacai.stanlee.omega.input

import scala.beans.BeanProperty


/**
  * @author manshahua@wacai.com
  * @date 2017/12/13 下午4:50
  */
class FieldInfo {

  @BeanProperty
  var fieldName: String = _

  @BeanProperty
  var fieldType: String = _
  @BeanProperty
  var isPrimaryKey: Boolean = _


  def this(fieldName: String, fieldType: String, isPrimaryKey: Boolean) = {
    this()
    this.fieldName = fieldName
    this.fieldType = fieldType
    this.isPrimaryKey = isPrimaryKey
  }

}

