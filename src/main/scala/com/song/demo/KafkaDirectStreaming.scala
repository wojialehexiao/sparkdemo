package com.song.demo

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.kafka.{KafkaManager, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/** *****************************************************************************
  * Copyright (c) 2017 daixinlian.com
  *
  * All rights reserved. 
  *
  * Contributors:
  * Song Xikun - Initial implementation
  * 2018/3/22 0022
  * ******************************************************************************/
object KafkaDirectStreaming {

  val updateFunc = (iter:Iterator[(String, Seq[Int], Option[Int])]) => {
    iter.map(x=>(x._1,x._2.sum + x._3.getOrElse(0)))
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("KafkaDirectStreaming")
                              .setMaster("local[*]")

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(5))

    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> "192.168.10.22:6667,192.168.10.23:6667,192.168.10.24:6667",
      "group.id" -> "test",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG->"false",
      "auto.offset.reset" -> "smallest"
    )

    val km = new KafkaManager(kafkaParams)

    val topics = Set("test")

    val df = km.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,kafkaParams,topics)

    df.foreachRDD(rdd => {
      if(!rdd.isEmpty()){

        println(rdd.flatMap(_._2.split(" ")).map((_,1)).reduceByKey(_+_).collect.mkString("|"))
        km.updateZKOffsets(rdd)
      }
    })

    ssc.start()

    ssc.awaitTermination()

  }
}
