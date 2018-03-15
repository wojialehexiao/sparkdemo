package com.song.demo

import org.apache.spark.{SparkConf, SparkContext}

/** *****************************************************************************
  * Copyright (c) 2017 daixinlian.com
  *
  * All rights reserved. 
  *
  * Contributors:
  * Song Xikun - Initial implementation
  * 2018/3/14 0014
  * ******************************************************************************/

object OrderingContext {
  implicit val orderAnimal = new Ordering[Animal]{
    override def compare(x: Animal, y: Animal): Int = {
      -(y.faceValue - x.faceValue)
    }
  }
}

object SortDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SortDemo").setMaster("local")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(List(("cat",10,99),("dog",12,90),("mouse",15,90),("pig",5,80),("monkey",13,85)))
    import OrderingContext._
    val rdd1 = rdd.sortBy(x=>Animal(x._1,x._2,x._3))
    val res = rdd1.collect
    println(res.mkString("\n"))


    sc.stop()
  }
}

//case class Animal(val name:String, val age:Int,val faceValue:Int) extends Ordered[Animal] with Serializable{
//  override def compare(that: Animal): Int = {
//    val i = that.faceValue - this.faceValue
//    if(i != 0){
//      i
//    }else{
//      that.age - this.age
//    }
//  }
//}

case class Animal(val name:String, val age:Int,val faceValue:Int) extends Serializable

