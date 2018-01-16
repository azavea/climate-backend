package com.azavea.climate

import Operations.KV
import Operations.Dictionary

import java.time.{ ZonedDateTime, ZoneId, ZoneOffset }


object Dividers {

  def divideByInfinity(collection: Seq[KV]): Map[ZonedDateTime, Seq[KV]] =  {
    val sorted = collection
      .sortBy({ kv => kv._1.time.toEpochSecond })
    val time = (sorted.head)._1.time
    Map(time -> sorted)
  }

  /**
    * An example of the `divide` lambda from the `query` function.
    * Divides a collection of key-value pairs into month-long
    * divisions.
    */
  def divideByCalendarMonth(collection: Seq[KV]): Map[ZonedDateTime, Seq[KV]] = {
    collection.groupBy({ kv =>
      val time = kv._1.time
      val year: Int = time.getYear
      val month: Int = time.getMonth.getValue
      val zone: ZoneId = time.getZone
      ZonedDateTime.of(year, month, 1, 0, 0, 0, 0, zone)
    })
      .map({ case (zdt, kvs) =>
        zdt -> kvs.sortBy({ kv => kv._1.time.toEpochSecond }) })
  }

  /**
    * An example of the `divide` lambda from the `query` function.
    * Divides a collection of key-value pairs into year-long
    * divisions.
    */
  def divideByCalendarYear(collection: Seq[KV]): Map[ZonedDateTime, Seq[KV]] = {
    collection.groupBy({ kv =>
      val time = kv._1.time
      val year: Int = time.getYear
      val zone: ZoneId = time.getZone
      ZonedDateTime.of(year, 1, 1, 0, 0, 0, 0, zone)
    })
      .map({ case (zdt, kvs) =>
        zdt -> kvs.sortBy({ kv => kv._1.time.toEpochSecond }) })
  }

}
