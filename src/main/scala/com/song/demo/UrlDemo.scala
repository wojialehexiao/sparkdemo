package com.song.demo

import java.net.URL

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

/** *****************************************************************************
  * Copyright (c) 2017 daixinlian.com
  *
  * All rights reserved. 
  *
  * Contributors:
  * Song Xikun - Initial implementation
  * 2018/3/13 0013
  * ******************************************************************************/
object UrlDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("urlDemo").setMaster("local")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile("E:/BaiduNetdiskDownload/资料文件/day29/itcast.log")
    val rdd1 = rdd.map(line=>{
      val f = line.split("\t")
      (f(1),1)
    })

    val rdd2 = rdd1.reduceByKey(_+_)
    val rdd3 = rdd2.map(x=>{
      val urlStr = x._1
      val url = new URL(urlStr)
      val host = url.getHost
      (host,(x._1,x._2))
    })

    val rdd4 = rdd3.sortBy(_._2._2,false).groupByKey()
    val rdd5 = rdd4.mapValues(iter=>{
      val sum = iter.map(_._2).sum
      (iter,sum)
    })

    val rdd6 = rdd5.map(x=>{

      (x._1,x._2._1.take(3),x._2._2)
    })

//    val res = rdd3.collect()
//    println(rdd3.collect.mkString("\n"))

//    val rdd7 = rdd3.sortBy(_._2._2,false)

    val hosts = rdd3.map(_._1).distinct.collect

    rdd3.partitionBy(new HostPartitioner(hosts))
//    println(rdd3.collect.mkString("\n"))

    rdd3.partitionBy(new HostPartitioner(hosts)).mapPartitions(iter=>{
      iter.toList.sortBy(_._2._2).reverse.take(3).toIterator
    }).map(x=>{
      x._1 + "\t" + x._2._1 + "\t" + x._2._2
    }).saveAsTextFile("d:/out12")
    sc.stop()
  }
}

class HostPartitioner(hosts : Array[String]) extends Partitioner{

  val hostMap = new mutable.HashMap[String,Int]()

  for(i <- 0 to hosts.length-1){
    hostMap += (hosts(i)->i)
  }
  override def numPartitions: Int = hosts.length

  override def getPartition(key: Any): Int = {
    hostMap.getOrElse(key.toString,0)
  }
}

