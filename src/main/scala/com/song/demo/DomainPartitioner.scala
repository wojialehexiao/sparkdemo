package com.song.demo

import java.net.URL

import org.apache.spark.Partitioner

class DomainPartitioner(number: Int) extends Partitioner {


  override def numPartitions: Int = number

  override def getPartition(key: Any): Int = {
    val domain = new URL(key.toString).getHost
    val code = domain.hashCode % number
    if (code < 0) {
      code + number
    } else {
      code
    }
  }

  override def equals(obj: Any): Boolean = obj match {
    case paritioner: DomainPartitioner =>
      numPartitions == paritioner.getPartition()
    case _ => false
  }
}
