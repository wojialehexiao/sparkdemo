package com.song.demo

import java.net.InetSocketAddress

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/** *****************************************************************************
  * Copyright (c) 2017 daixinlian.com
  *
  * All rights reserved. 
  *
  * Contributors:
  * Song Xikun - Initial implementation
  * 2018/3/22 0022
  * ******************************************************************************/
object FlumePollStreaming {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FlumePollStreaming").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(5))

    val address = Array(new InetSocketAddress("master",8888))
    val df = FlumeUtils.createPollingStream(ssc,address,StorageLevel.MEMORY_AND_DISK)

    val words = df.flatMap(x=>{
      new String(x.event.getBody.array()).split(" ")
    }).map((_,1))

    val result = words.reduceByKey(_+_)
    result.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
