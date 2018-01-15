package com.azavea.climate

import Operations.KV
import Operations.Dictionary

import java.time.{ ZonedDateTime, ZoneId, ZoneOffset }


object Dividers {

  def divideByInfinity(collection: Seq[KV]): Map[ZonedDateTime, Seq[KV]] =  {
    val time = collection.map({ kv => kv._1.time }).sortBy({ k => k.toEpochSecond }).head
    Map(time -> collection)
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
  }

  // /**
  //   * This function can be partially-applied to produce a divider of
  //   * any number of days (which does not respect calendar months,
  //   * years, &c).
  //   */
  // def divideByDays(days: Int)(collection: Seq[KV]): Map[ZonedDateTime, Seq[KV]] = {
  //   collection.sortBy({ kv => kv._1.time.toInstant.getEpochSecond })
  //     .grouped(days)
  //     .map({ seq => seq.head._1.time -> seq })
  //     .toMap
  // }

}
