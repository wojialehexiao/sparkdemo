package com.song.demo

import scala.beans.BeanProperty

/** *****************************************************************************
  * Copyright (c) 2017 daixinlian.com
  *
  * All rights reserved. 
  *
  * Contributors:
  * Song Xikun - Initial implementation
  * 2018/3/13 0013
  * ******************************************************************************/
object ScalaTest {

  def main(args: Array[String]): Unit = {

    val a1 = new Aa
    a1.b(11)
    a1.a(22)
    a1.b = (x:Int)=>println(x)
//    a1.a = (x:Int)=>println(x)

    a1.age = 29

    a1.getName
    a1.setName("ssss")
    a1.age_=(3333)

    val bbbb = new a1.Bb

    Cc.c(1)
  }
}

class Aa{

  @BeanProperty var name:String = _

  private[demo] var age:Int = _
  def a(x:Int) = println(x)
  var b = (x:Int)=>println(x)

  class Bb{
    var sex:String = _
  }
}

object Cc{
  def c(x:Int)=println(x)
}

class Cc{
  def cc() = {
    Cc.c(2)
  }
List
  def main(args: Array[String]): Unit = {

    println("dddddddddddddddddddd")
  }
}