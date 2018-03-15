package com.song.demo

/** *****************************************************************************
  * Copyright (c) 2017 daixinlian.com
  *
  * All rights reserved. 
  *
  * Contributors:
  * Song Xikun - Initial implementation
  * 2018/3/14 0014
  * ******************************************************************************/
import java.sql.{Date, DriverManager}

import com.mysql.jdbc.Connection
import org.apache.spark.{SparkConf, SparkContext}

import scala.io._


object IpDemo {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("ipDemo").setMaster("local")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile("E:/BaiduNetdiskDownload/资料文件/day30/计算IP地址归属地/ip.txt")

    /**
      * 设置广播变量
      */
    val ipBroadCast =  sc.broadcast(rdd.collect)

    val rdd2 = sc.textFile("E:/BaiduNetdiskDownload/资料文件/day30/计算IP地址归属地/20090121000132.394251.http.format")
    val rdd3 = rdd2.map(x=>{
      val accessArr = x.split("\\|")
      val ip = accessArr(1)
      val ipRes = binarySearch(ipBroadCast.value,ip).split("\\|")
      (ip,accessArr(2),ipRes(4),ipRes(5),ipRes(6),ipRes(7))
    })

    val rdd4 = rdd3.map(x=>(x _5, 1)).reduceByKey(_+_)
    rdd4.foreachPartition(iter=>{


      val driver = "com.mysql.jdbc.Driver"
      val url = "jdbc:mysql://192.168.10.4:3309/test"
      val username = "jdbcuser"
      val password = "jdbc88142E10.4"

      // do database insert
      Class.forName(driver)
      val connection = DriverManager.getConnection(url, username, password)
      try {
        iter.foreach(x=>{
          val prep = connection.prepareStatement("INSERT INTO testv (city, visitNum, createTime) VALUES (?, ?, ?) ")
          prep.setString(1, x._1)
          prep.setInt(2, x._2)
          prep.setDate(3,new Date(System.currentTimeMillis()))
          prep.executeUpdate
        })

      }
      finally {
        connection.close
      }
    })

    //println(rdd4.collect.mkString("\n"))

    sc.stop()
  }


  def ipToLong(ipAddress: String): Long = {
    var result = 0L
    val ipAddressInArray = ipAddress.split("\\.")
    var i = 3
    while ({ i >= 0 }) {
      val ip = ipAddressInArray(3 - i).toLong
      result |= ip << (i * 8)

      {
        i -= 1; i + 1
      }
    }
    result
  }

//  def binarySearch(ip:String): String = {
//    val source = Source.fromFile("E:/BaiduNetdiskDownload/资料文件/day30/计算IP地址归属地/ip.txt")
//    val ipArr = source.getLines().toArray
//    source.close
//
//    binaySearch(ipArr,0,ipArr.length-1,ip)
//  }

  def binarySearch(ipArr:Array[String],ipAddr:String): String ={
    binaySearch(ipArr,0,ipArr.length-1,ipAddr)
  }

  def binaySearch(ipArr:Array[String],start:Int,end:Int,ipAddr:String): String ={
    val ip = ipToLong(ipAddr)
    val postion = (start + end) / 2

    val startIp = ipArr(start).split("\\|")(2).toLong
    val endIp = ipArr(end).split("\\|")(2).toLong

    val posIp = ipArr(postion).split("\\|")(2).toLong
    val nextposIp = ipArr(postion+1).split("\\|")(2).toLong

    if(ip >= posIp && ip < nextposIp){
      ipArr(postion)
    }else if(nextposIp == ip){
      ipArr(postion+1)
    }else if(ip < posIp){
      binaySearch(ipArr,start,postion,ipAddr)
    }else if(ip > nextposIp){
      binaySearch(ipArr,postion+1,end,ipAddr)
    }else{
      "not found"
    }
  }
}
