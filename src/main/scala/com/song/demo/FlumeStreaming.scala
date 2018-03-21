package com.song.demo

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.{SparkConf, SparkContext}

/** *****************************************************************************
  * Copyright (c) 2017 daixinlian.com
  *
  * All rights reserved. 
  *
  * Contributors:
  * Song Xikun - Initial implementation
  * 2018/3/21 0021
  * ******************************************************************************/
object FlumePushStreaming {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FlumeStreaming").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(5))

    val flumeStream = FlumeUtils.createStream(ssc,"localhost",8888)

    val words = flumeStream.flatMap(x=>{
      println("***************************************")
      new String(x.event.getBody.array()).split(" ")}).map((_,1))
    val results = words.reduceByKey(_+_)


    results.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
