package com.wacai.stanlee.omega.input

/**
  * @author manshahua@wacai.com
  * @date 2017/12/13 下午7:26
  */


import scala.beans.BeanProperty

class CategoricalSliceDO {

  @BeanProperty
  var categoricalValues: List[String] = _

  @BeanProperty
  var fieldName: String = _

  @BeanProperty
  var fieldType: String = _

}


