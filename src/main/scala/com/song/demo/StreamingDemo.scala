package com.song.demo

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/** *****************************************************************************
  * Copyright (c) 2017 daixinlian.com
  *
  * All rights reserved. 
  *
  * Contributors:
  * Song Xikun - Initial implementation
  * 2018/3/20 0020
  * ******************************************************************************/
object StreamingDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StreamingDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc,Seconds(5))

    /**
      * DSStream 是一个特殊的RDD
      */
    val ds = ssc.socketTextStream("master",8888)

    val result = ds.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    result.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
