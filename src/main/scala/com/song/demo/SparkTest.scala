package com.song.demo

import org.apache.spark.{SparkConf, SparkContext}

/** *****************************************************************************
  * Copyright (c) 2017 daixinlian.com
  *
  * All rights reserved. 
  *
  * Contributors:
  * Song Xikun - Initial implementation
  * 2018/3/13 0013
  * ******************************************************************************/
object SparkTest {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("sparktest").setMaster("local")
    val sc = new SparkContext(conf)

//    val rdd1 = sc.parallelize(List(1,2,3,4,5,6),2)

//    rdd1.mapPartitionsWithIndex((x,iter)=>{
//
//      println(x)
//
//      iter
//    }).collect

//    val rdd = sc.parallelize(List("cat","dog","mouse","monkey","duck"))
//    val rdd1 = rdd.keyBy(_.length)
//    val rdd2 = rdd1.map((x)=>(x._2,x._1))
//    val rdd3 = rdd2.keys
//    val rdd4 = rdd2.values
//    println(rdd2.collect.mkString(","))
//    println(rdd3.collect.mkString(","))
//    println(rdd4.collect.mkString(","))
//    println(rdd2.collectAsMap)
//    println(rdd2.countByKey())
//    println(rdd2.countByValue())
//    println(rdd2.filterByRange("b","d").collectAsMap())
//    println(rdd2.mapValues(_*10).collectAsMap())

//    val rdd = sc.parallelize(List(("cat","1 2"),("dog","3 4"),("monkey","5 6"),("duck","7 8"),("pig","9 0")))
//    val rdd1 = rdd.flatMapValues(_.split(" ")).mapValues(_.toInt)
//
//    val rdd2 = rdd1.foldByKey(0)(_+_)
//    println(rdd2.collect.mkString("|"))

    val rdd = sc.parallelize(List(1,2,3,3,4,5,5,6,6,7,8,9,90,3),3)
    rdd.foreachPartition(iter=>{

      println(iter.mkString("|"))
    })

    Iterable()

    sc.stop()
  }
}
