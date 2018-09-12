package com.wacai.stanlee.omega.bang

import scala.beans.BeanProperty
import scala.collection.mutable

/**
  * @author manshahua@wacai.com
  * @date 2017/12/16 下午8:59
  */
class SqlMaker {
  //最终所有的字段的生成sql
  @BeanProperty
  var finalSqlString:List[String] = List()
  //把每个字段和每个字段对应的且分段放到一个map里面
  @BeanProperty
  var fieldNameSlices:mutable.Map[String,BangField] = mutable.Map()
  @BeanProperty
  var fieldGenerationSql:Map[String, String] = Map()
  //通过新生成的字段（各个切片字段）和对某些字段采取某些聚合函数生成核心的sql语句
  def makeCoreSql(fieldAggMethods:Map[String, List[String]], fieldList:List[List[String]]):List[String]= {
//    var finalSqlString: List[String] = List()
    for ((k, v) <- fieldAggMethods) {
      v.foreach(method => {
        val methodTrans = {
          method match {
            case "c" => "count ("
            case "cd" => "count (distinct "
            case "sum" => "sum ("
            case "avg" => "avg ("
            case "max" => "max ("
            case "min" => "min ("
            case _ => throw new Exception("error method \n")
          }
        }
        //case when a and b then calc field else null end
        if(Nil==fieldList){
          val str = method+"_"+k
          val sql = "coalesce("+methodTrans +  k + " ),0) as " + str
          finalSqlString = finalSqlString :+sql
          val bangField:BangField = new BangField(method, k, Nil)
          fieldGenerationSql+=(str->sql)
          fieldNameSlices(str) = bangField
        }else {
          fieldList.foreach(f => {
            val str = method + "_" + f.mkString("_") + "_" + k
            val sql = "coalesce(" + methodTrans + " case when " + f.mkString(" and ") + " then " + k + " else null end ),0) as " + str
            finalSqlString = finalSqlString :+ sql
            val bangField: BangField = new BangField(method, k, f)
            fieldGenerationSql += (str -> sql)
            fieldNameSlices(str) = bangField
          })
        }
      })
    }
    finalSqlString
  }
}
