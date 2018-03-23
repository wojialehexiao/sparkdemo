package com.song.demo

import kafka.log.Log
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.kafka.KafkaUtils
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
object KafkaStreaming {

  val updateFunc = (iter:Iterator[(String, Seq[Int], Option[Int])]) => {
    iter.map(x=>(x._1,x._2.sum + x._3.getOrElse(0)))
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("KafkaStreaming")
                              .setMaster("local[*]")

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(5))
    ssc.checkpoint("d:/checkpoint")

    val zookeeper = "192.168.10.22:2181,192.168.10.23:2181,192.168.10.24:2181"

    val group = "test"

    val topicMap = Map("test"->3)

    val df  = KafkaUtils.createStream(ssc,zookeeper,group,topicMap)
    val result = df.flatMap(_._2.split(" ")).map((_,1)).reduceByKey(_+_).updateStateByKey(updateFunc,new HashPartitioner(sc.defaultParallelism),true)
    result.print()

    ssc.start()

    ssc.awaitTermination()

  }
}
