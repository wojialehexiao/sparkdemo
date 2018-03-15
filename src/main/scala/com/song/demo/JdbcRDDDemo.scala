package com.song.demo

import java.sql.{Connection, DriverManager}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.JdbcRDD

/** *****************************************************************************
  * Copyright (c) 2017 daixinlian.com
  *
  * All rights reserved. 
  *
  * Contributors:
  * Song Xikun - Initial implementation
  * 2018/3/15 0015
  * ******************************************************************************/
object JdbcRDDDemo {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("JdbcRDDDemo").setMaster("local")
    val sc = new SparkContext(conf)

    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://192.168.10.4:3309/test"
    val username = "jdbcuser"
    val password = "jdbc88142E10.4"

    Class.forName(driver)
    val connection = DriverManager.getConnection(url, username, password)

    val rdd = new JdbcRDD(sc,getConnection,"select city,visitNum,createTime from testv where id > ? and id < ?",0L,100L,2,rs=>{
      (rs.getString(1),
      rs.getInt(2),
      rs.getDate(3))
    })

    println(rdd.collect().mkString("\n"))
  }

  val getConnection = () => {
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://192.168.10.4:3309/test"
    val username = "jdbcuser"
    val password = "jdbc88142E10.4"

    Class.forName(driver)
    DriverManager.getConnection(url, username, password)
  }
}
