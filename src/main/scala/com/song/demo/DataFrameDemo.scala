package com.song.demo

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

/** *****************************************************************************
  * Copyright (c) 2017 daixinlian.com
  *
  * All rights reserved. 
  *
  * Contributors:
  * Song Xikun - Initial implementation
  * 2018/3/19 0019
  * ******************************************************************************/
object DataFrameDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DataFrameDemo").setMaster("local")
    val sc = new SparkContext(conf)

    System.setProperty("HADOOP_USER_NAME","hadoop")
    val sqlContext = new SQLContext(sc)

    val rdd = sc.textFile("hdfs://master:9000/t_user").map(_.split("\t"))

    /**
      * 通过反射方式获取DataFrame
      */

//    val personRDD = rdd.map(x=>Person(x(0).toLong,x(1),x(2).toInt))
//    import sqlContext.implicits._
//    val personDF = personRDD.toDF

    /**
      * 通过外部load方式获取
      */
    val personDF = sqlContext.load("hdfs://master:9000/save")

    /**
      * 通过StructType方式获取DataFrame
      */
//    val schema = StructType(List(
//      StructField("id",LongType,true),
//      StructField("name",StringType,true),
//      StructField("age",IntegerType,true)
//    ))

//    val personRDD = rdd.map(x=>Row(x(0).toLong,x(1),x(2).toInt))
//    val personDF = sqlContext.createDataFrame(personRDD,schema)
    personDF.createOrReplaceTempView("t_person")
//    personDF.map()

//    sqlContext.sql("select * from t_person").write.json("hdfs://master:9000/json")
//    sqlContext.sql("select * from t_person").write.save("hdfs://master:9000/save")
    sqlContext.sql("select * from t_person").show()


    sc.stop()
  }
}


case class Person(id:Long,name:String,age:Int)