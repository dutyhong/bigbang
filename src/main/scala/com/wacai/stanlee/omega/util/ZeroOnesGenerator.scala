package com.wacai.stanlee.omega.util

/**
  * @author manshahua@wacai.com
  * @date 2017/12/15 下午4:13
  */
object ZeroOnesGenerator {

  def zerosGenerate(n: Int): String = {
    if (n <= 0) println("ZerosGenerator error")
    var tmp: String = ""
    for (i <- 0 until n) {
      tmp = tmp + "0"
    }
    tmp
  }

  def main(args: Array[String]): Unit = {
    println(zerosGenerate(2))
  }

}
