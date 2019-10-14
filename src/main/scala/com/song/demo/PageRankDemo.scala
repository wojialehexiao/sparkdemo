package com.song.demo

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object PageRankDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("PageRankDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val links = sc.textFile("./data/pageRank.txt")
      .map(x => {
        val split = x.split(":")
        val key = split(0)
        val value = split(1).split(",")
        (key, value)
      })
      .partitionBy(new HashPartitioner(2))
      .persist()

    var ranks = links.mapValues(v => 1.0)


    val c = links.join(ranks).flatMap {
      case (pageId, (link, rank)) =>
        link.map(dest => (dest, rank / link.size))
    }


    for (i <- 0 until 10) {

      val contributions = links.join(ranks).flatMap {
        case (pageId, (link, rank)) => link.map(dest => (dest, rank / link.size))
      }
      ranks = contributions.reduceByKey((x, y) => x + y).mapValues(v => 0.15 + 0.85 * v)
    }

//    ranks.sortByKey(ascending = false)

    ranks.map(x => x._1 + "," + x._2).saveAsTextFile("./data/pageRankOut.txt")

    sc.stop()


  }

}
