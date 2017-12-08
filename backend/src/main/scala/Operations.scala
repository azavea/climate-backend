package com.azavea.climate

import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.s3._
import geotrellis.vector._
import geotrellis.vector.io._

import scala.concurrent._

import java.time.{ ZonedDateTime, ZoneId, ZoneOffset }
import java.util.concurrent.Executors


object Operations {

  val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(Runtime.getRuntime.availableProcessors)) // XXX
  val rng = new scala.util.Random // XXX

  type KV = (SpaceTimeKey, MultibandTile)
  type Dictionary = Map[String, Double]

  val geojsonUri = "./geojson/Los_Angeles.geo.json"
  val polygon =
    scala.io.Source.fromFile(geojsonUri, "UTF-8")
      .getLines
      .mkString
      .extractGeometries[MultiPolygon]
      .head

  val bucket = "ingested-gddp-data"
  val prefix = "rcp85_r1i1p1_CanESM2"
  val as = S3AttributeStore(bucket, prefix)
  val id = LayerId("rcp85_r1i1p1_CanESM2_benchmark_64_years_temp", 0)
  val dataset = S3CollectionLayerReader(as)

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
    * An example of the `areaToDictionary` lambda from the `query`
    * function.  Converts an area (a tile) into a dictionary of the
    * form Map("tasmin" -> x, "tasmax" -> y).
    */
  def areaToTasminTasmax(area: MultibandTile): Dictionary = {
    var count: Int = 0
    var tasmin: Double = 0.0
    var tasmax: Double = 0.0

    area.band(0).foreachDouble({ z: Double =>
      if (!isNoData(z)) {
        count = count + 1
        tasmin = tasmin + z
      }
    })
    area.band(1).foreachDouble({ z: Double =>
      if (!isNoData(z)) {
        tasmax = tasmax + z
      }
    })
    tasmin /= count
    tasmax /= count

    Map("tasmin" -> tasmin, "tasmax" -> tasmax)
  }

  /**
    * An example of the `dictionariesToScalers` lambda from the
    * `query` function.  Takes a sequence of dictionaries (from a
    * temporal division) and produces a sequence of (one or more)
    * scalers.
    */
  def maxTasmax(dictionaries: Seq[Dictionary]): Seq[Double] = {
    List(
      dictionaries
        .map({ d => d.getOrElse("tasmin", throw new Exception) })
        .reduce({ (a, x) => math.max(a, x) })
    )
  }

  /**
    * Perform a query, wrap the computation in a future for asynchrony.
    */
  def futureQuery(
    startTime: ZonedDateTime, endTime: ZonedDateTime, area: MultiPolygon,
    divide: Seq[KV] => Map[ZonedDateTime, Seq[KV]],
    areaToDictionary: MultibandTile => Dictionary,
    dictionariesToScalers: Seq[Dictionary] => Seq[Double]
  ): Future[Map[ZonedDateTime, Seq[Double]]] = {
    Future{ query(startTime, endTime, area, divide, areaToDictionary, dictionariesToScalers) }(ec)
  }

  /**
    * Perform a query.
    *
    * @param  startTime              The beginning of the temporal search range
    * @param  endTime                The end of the temporal search range
    * @param  area                   The spatial search range (given as a multipolygon)
    * @param  divide                 A function that takes a sequence of (key, tile) pairs a divides it into appropriate temporally-contiguous divisions
    * @param  areaToDictionary       A function that turns an area (a tile) into a dictionary of variables to their values, e.g. Map("tasmin" -> 1, "tasmax" -> 3).
    * @param  dictionariesToScalers  A function that turns a sequence of dictionaries into a sequence of scalers
    * @return                        A map from dates to sequences of doubles.  The dates mark the beginnings of temporal divisions and the sequences of one or more scalers are the values derived from the respective chunks.
    */
  def query(
    startTime: ZonedDateTime, endTime: ZonedDateTime, area: MultiPolygon,
    divide: Seq[KV] => Map[ZonedDateTime, Seq[KV]],
    areaToDictionary: MultibandTile => Dictionary,
    dictionariesToScalers: Seq[Dictionary] => Seq[Double]
  ): Map[ZonedDateTime, Seq[Double]] = {
    val collection = dataset
      .query[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]](id)
      .where(Intersects(area))
      .where(Between(startTime, endTime))
      .result.mask(area)

    divide(collection)
      .map({ t => (t._1, t._2.map({ kv => areaToDictionary(kv._2) })) })
      .map({ t => (t._1, dictionariesToScalers(t._2)) })
      .toMap
  }

  def benchmark(): Unit = {
    val years = rng.nextInt(20)
    val startTime = ZonedDateTime.of(2018, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)
    val endTime = ZonedDateTime.of(2018+years, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)

    val beforeMillis = System.currentTimeMillis
    val collection = query(startTime, endTime, polygon, divideByCalendarMonth, areaToTasminTasmax, maxTasmax)
    val afterMillis = System.currentTimeMillis

    println(s"${years}, ${afterMillis - beforeMillis}")
  }

  def main(args: Array[String]) : Unit = {
    val times =
      if (args.length > 0) args(0).toInt
      else 1000
    println(times)

    var i = 0
    while (i < times) {
      benchmark
      i = i + 1
    }
  }

}
