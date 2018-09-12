package com.wacai.stanlee.omega.util

/**
  * @author manshahua@wacai.com
  * @date 2017/12/15 下午4:15
  */
object DecimalConvertor {
  def decimalToBinary(value: Int, len: Int): String = {
    var binaryString: String = java.lang.Integer.toBinaryString(value)
    val binaryLen: Int = binaryString.length
    if (binaryLen < len) {
      val diffLen: Int = len - binaryLen
      val zeros: String = ZeroOnesGenerator.zerosGenerate(diffLen)
      binaryString = zeros + binaryString
    }
    binaryString
  }

  def generatorBinaryList(maxValue: Int, strLen: Int): List[String] = {
    var binaryList: List[String] = List()
    0.to(maxValue-1).map(x=> {
      val tmpStr: String = decimalToBinary(x + 1, strLen)
      tmpStr
    }).toList
//    binaryList
  }

  def main(args: Array[String]): Unit = {
    val tmpStr: String = decimalToBinary(1, 3)
    val str: List[String] = generatorBinaryList(7, 3)
    println(tmpStr)
    println(str)
  }
}
