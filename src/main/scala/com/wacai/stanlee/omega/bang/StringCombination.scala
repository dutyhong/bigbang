package com.wacai.stanlee.omega.bang

import com.wacai.stanlee.omega.util.DecimalConvertor

import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.util.control.Breaks._
/**
  * @author manshahua@wacai.com
  * @date 2017/12/15 下午12:52
  */
object StringCombination {
  //输出所有的组合
  def combinate(input: List[String]): List[List[String]] = {
    //首先产生二进制的位数代表每个index的str是否组合
    val size: Int = input.size
    val len: Int = Math.pow(2.0, size).toInt - 1
    val binaryList: List[String] = DecimalConvertor.generatorBinaryList(len, size)
    //所有的组合为按照二进制的1 0进行index输出
    val combinationSize: Int = binaryList.size
    var combinations: List[List[String]] = List()
    for (i <- 0 until combinationSize) {
      val oneBinary: String = binaryList(i)
      var oneCombination: List[String] = List()
      for (j <- 0 until size if "1" == oneBinary.substring(j, j + 1)) {
        oneCombination = oneCombination :+ input(j)
      }
      combinations = combinations :+ oneCombination
    }
    combinations
  }
  //输出所有的只包含两个元素的组合情况
  def binaryCombinate(input: List[String]): List[List[String]] = {
//    //首先产生二进制的位数代表每个index的str是否组合
//    val size: Int = input.size
//    val len: Int = Math.pow(2.0, size).toInt - 1
//    val binaryList: List[String] = DecimalConvertor.generatorBinaryList(len, size)
//    //所有的组合为按照二进制的1 0进行index输出
//    val combinationSize: Int = binaryList.size
//    var combinations: List[List[String]] = List()
//
//    for (i <- 0 until combinationSize) {
//      val oneBinary: String = binaryList(i)
//      breakable {
//        if (countOneCnt(oneBinary) != 2) break
//        var oneCombination: List[String] = List()
//        for (j <- 0 until size if "1" == oneBinary.substring(j, j + 1)) {
//          oneCombination = oneCombination :+ input(j)
//        }
//        combinations = combinations :+ oneCombination
//      }
//    }
    val size:Int = input.size
    val ccnm:Ccnm = new Ccnm(size, 2)
    val indices:mutable.Buffer[String]  = ccnm.tt()
    val combinations = indices.map(ind=>{
      val tmpInds:List[String] = ind.split(";").toList
      List(input(Integer.parseInt(tmpInds(0))), input(Integer.parseInt(tmpInds(1))))
    })
    combinations.toList
  }

  def crossJoin[T](list: List[List[T]]): List[List[T]] = {
    list match {
      case Nil=> Nil
      case xs :: Nil => xs map (List(_))
      case x :: xs => for {
        i <- x
        j <- crossJoin(xs)
      } yield List(i) ++ j
    }
  }

  //统计一个二进制表示中1的个数
  def countOneCnt(binaryCode:String): Int =
  {
    val length:Int = binaryCode.length
    var num:Int = 0
    0.until(length).foreach(ind=>{
      if ("1".equals(binaryCode.substring(ind,ind+1)))
        num = num +1
    })
    num
  }
    def main(args: Array[String]): Unit = {
      val list: List[String] = List("a", "b","c","d","e","f","g","h","i")
      println(binaryCombinate(list))
      val binaryCode = "1010011"
      println(countOneCnt(binaryCode))
    }
}
