package com.song.demo

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object IdToInt {

  def main(args: Array[String]): Unit = {

    val entPath = args(0)
    val personPath = args(1)
    val core = args(2).toInt

    val conf = new SparkConf().setAppName("IdToInt").setMaster("local[" + core + "]")
    val sc = new SparkContext(conf)

    val entInvRdd = sc.textFile(entPath)
    val personInvRdd = sc.textFile(personPath)


    val person2Ent = personInvRdd.map(x => {
      val split = x.split("\\|")
      (split(0), split(1))
    })


    val distinct = person2Ent.values.union(entInvRdd.flatMap(x => {
      val split = x.split("\\|")
      List(split(0), split(1))
    })).distinct

    val entZipRdd = distinct.zipWithIndex().partitionBy(new HashPartitioner(core)).persist()

    val personZipRdd = person2Ent.keys.distinct.zipWithIndex().partitionBy(new HashPartitioner(core)).persist()

    println(entZipRdd.collect.toList)
    println(personZipRdd.collect.toList)

    val newEntInvRdd = entInvRdd.map(x => {
      val split = x.split("\\|")
      val startId = split(0)
      val endId = split(1)
      val prop1 = split(2)
      val prop2 = split(3)
      EntInv(startId, endId, prop1, prop2, 0, 0)
    })


    val startIdEntInvRdd = newEntInvRdd.map(entInv => (entInv.startId, entInv))
    val startIdConvertedEntInvRdd = entZipRdd.join(startIdEntInvRdd).map({
      case (_: String, (newStartId: Long, entInv: EntInv)) => {
        EntInv("", entInv.endId, entInv.prop1, entInv.prop2, newStartId, 0)
      }
    })

    val endIdEntInvRdd = startIdConvertedEntInvRdd.map(entInv => (entInv.endId, entInv))


    val convertedEntInvRdd = entZipRdd.join(endIdEntInvRdd).map({
      case (_: String, (newEndId: Long, entInv: EntInv)) => {
        EntInv("", "", entInv.prop1, entInv.prop2, entInv.newStartId, newEndId)
      }
    })

    println(convertedEntInvRdd.collect.toList)

    val newPersonInvRdd = personInvRdd.map(x => {
      val split = x.split("\\|")
      PersonInv(split(0), split(1), split(2), split(3), 0, 0)
    })


    val startIdPersonInvRdd = newPersonInvRdd.map(personInv => (personInv.startId, personInv))
    val startIdConvertedPersonInvRdd = personZipRdd.join(startIdPersonInvRdd).map({
      case (_: String, (newStartId: Long, personInv: PersonInv)) => {
        PersonInv("", personInv.endId, personInv.prop1, personInv.prop2, newStartId, 0)
      }
    })

    val endIdPersonInvRdd = startIdConvertedPersonInvRdd.map(personInv => (personInv.endId, personInv))

    val convertedPersonInvRdd = entZipRdd.join(endIdPersonInvRdd).map({
      case (_: String, (newEndId: Long, personInv: PersonInv)) => {
        PersonInv("", "", personInv.prop1, personInv.prop2, personInv.newStartId, newEndId)
      }
    })


    convertedEntInvRdd.map(entInv => entInv.newStartId + "|" + entInv.newEndId + "|" + entInv.prop1 + "|" + entInv.prop2).saveAsTextFile("data/entInv")
    convertedPersonInvRdd.map(personInv => personInv.newStartId + "|" + personInv.newEndId + "|" + personInv.prop1 + "|" + personInv.prop2).saveAsTextFile("data/personInv")

    sc.stop()
  }


  case class EntInv(startId: String, endId: String, prop1: String, prop2: String, newStartId: Long, newEndId: Long)

  case class PersonInv(startId: String, endId: String, prop1: String, prop2: String, newStartId: Long, newEndId: Long)


}
