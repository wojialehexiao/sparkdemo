package com.song.demo

import org.apache.spark.{SparkConf, SparkContext}

/** *****************************************************************************
  * Copyright (c) 2017 daixinlian.com
  *
  * All rights reserved. 
  *
  * Contributors:
  * Song Xikun - Initial implementation
  * 2018/3/16 0016
  * ******************************************************************************/
object WordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wc").setJars(Array("F:\\ideaworkspace\\sparkdemo\\target\\sparkdemo-1.0-SNAPSHOT.jar")).setMaster("spark://master:7077")

//    val conf = new SparkConf().setAppName("wc").setMaster("spark://master:7077")
    val sc = new SparkContext(conf)

    sc.textFile(args(0))
      .flatMap(_.split(" "))
      .map((_,1))
      .reduceByKey(_+_)
        .sortBy(_._2)
      .saveAsTextFile(args(1))

    sc.stop()
  }
}
