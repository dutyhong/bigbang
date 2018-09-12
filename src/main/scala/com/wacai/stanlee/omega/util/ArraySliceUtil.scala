package com.wacai.stanlee.omega.util
import com.wacai.stanlee.omega.bang.StringCombination

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * @author manshahua@wacai.com
  * @date 2017/12/26 下午7:57
  */
object ArraySliceUtil {
  def getIntervalArray(minValue:Double, maxValue:Double, intervalNum:Int):mutable.Buffer[Double]={
    val interval = (maxValue-minValue)/intervalNum
    val intervals = 0.to(intervalNum).map(index=>{
      minValue+interval*index
    }).toBuffer
    intervals
  }
  def getCombinationCnt(input:Map[String,Int], aggMethodCnt:Int):Int={
    val keys = input.keySet.toList
    val combinations = StringCombination.combinate(keys)
    var intervals = 0
    combinations.map(list=>{
      var oneCombinationCnt = 1
      list.foreach(field=>{
        oneCombinationCnt = oneCombinationCnt*input.get(field).get
      })
      intervals = oneCombinationCnt +intervals
    })
    intervals*aggMethodCnt
  }

  def main(args: Array[String]): Unit = {
    val a = 1
    val b = 10
    val c = 9
    val intervals = getIntervalArray(1,10, 9)
    val result = StringCombination.combinate(List("a", "b", "c"))
    val test = Map("a"->2, "b"->6, "c"->12)//, "d"->4)
    val cnt = getCombinationCnt(test, 2)
    println(cnt)
    println(result.mkString(" "))
  }
}
