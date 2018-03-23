package com.song.demo

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.kafka.KafkaManager
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/** *****************************************************************************
  * Copyright (c) 2017 daixinlian.com
  *
  * All rights reserved. 
  *
  * Contributors:
  * Song Xikun - Initial implementation
  * 2018/3/23 0023
  * ******************************************************************************/
object WindowOption {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("WindowOption").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(5))

    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> "192.168.10.22:6667,192.168.10.23:6667,192.168.10.24:6667",
      "group.id" -> "test",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG->"false",
      "auto.offset.reset" -> "smallest"
    )

    val km = new KafkaManager(kafkaParams)

    val df = km.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,kafkaParams,Set("test"))
    val result = df.flatMap(_._2.split(" ")).map((_,1)).reduceByKeyAndWindow((a:Int,b:Int)=>a+b,Seconds(30),Seconds(10))
    result.print()

    df.foreachRDD(rdd=>{
      km.updateZKOffsets(rdd)
    })

    ssc.start()
    ssc.awaitTermination()

  }
}
