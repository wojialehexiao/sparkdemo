package com.song.demo

import java.util

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel



object RddOperatorDemo {



  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("RddOperatorDemo").setMaster("local")
    val sc = new SparkContext(conf)


    println("-------------RDD----------------")

    val rdd2 = sc.parallelize(List(1, 1, 2, 3, 4, 5, 1, 2))
    val sumN = rdd2.reduce((x, y) => x + y)

    println(sumN)

    rdd2.persist(StorageLevel.DISK_ONLY)
    val rdd4 = rdd2.sample(true, 0.5)

    println(rdd4.collect().toList)
    val foldSum = rdd2.fold(0)((x, y) => x + y)


    println(foldSum)
    val res = rdd2.aggregate((0, 0))(
      (acc: (Int, Int), item: Int) => (acc._1 + item, acc._2 + 1),
      (value1: (Int, Int), value2: (Int, Int)) => (value1._1 + value2._1, value1._2 + value2._2)
    )

    rdd2.sum()
    println(res)


    println("-------------PairRDD----------------")

    var pairRdd = sc.parallelize(List((1, 112), (2, 3), (1, 41), (2, 5), (3, 5)))
    pairRdd = pairRdd.repartition(4)

    val groupByKeyResult = pairRdd.groupByKey()
    println(groupByKeyResult.collect().toList)


    val foldByKeyResult = pairRdd.foldByKey(0)((x, y) => x + y)
    println(foldByKeyResult.collect().toList)


    val compbineByKeyResult = pairRdd.combineByKey(
      (value: Int) => (value, 1),
      (acc: (Int, Int), value: Int) => (acc._1 + value, acc._2 + 1),
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2),
      2
    );

    println(compbineByKeyResult.collect().toList)

    println(pairRdd.partitions.size)


    val pairRdd2 = sc.parallelize(List((1, "song"), (2, "wang")))

    val joinRdd = pairRdd.join(pairRdd2)

    println(joinRdd.collect.toList)

    val leftJoinRdd = pairRdd.leftOuterJoin(pairRdd2)

    println(leftJoinRdd.collect.toList)


    implicit val order = new Ordering[Int] {
      override def compare(x: Int, y: Int): Int = -x.toString.compare(y.toString)
    }

    val sortByKeyRdd = pairRdd.sortByKey()

    println(sortByKeyRdd.collect.toList)

    println(pairRdd.collectAsMap())

    val countByKeyResult = pairRdd.countByKey();

    println(countByKeyResult.toList)

    val lookupResult = pairRdd.lookup(1)

    println(lookupResult)

    val partitioner = pairRdd.partitioner;
    println(partitioner)


    val partitionRdd = pairRdd.partitionBy(new HashPartitioner(100))

    println(partitionRdd.partitioner)

    sc.stop()


  }

}
