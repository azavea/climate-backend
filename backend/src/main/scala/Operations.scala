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
    * Perform a query, wrap the computation in a future for
    * asynchrony.
    */
  def futureQuery(
    startTime: ZonedDateTime, endTime: ZonedDateTime, area: MultiPolygon,
    divide: Seq[KV] => Map[ZonedDateTime, Seq[KV]],
    narrower: MultibandTile => Dictionary,
    box: Seq[Dictionary] => Seq[Double]
  ): Future[Map[ZonedDateTime, Seq[Double]]] = {
    Future{ query(startTime, endTime, area, divide, narrower, box) }(ec)
  }

  /**
    * Perform a query.
    *
    * @param  startTime The beginning of the temporal search range
    * @param  endTime   The end of the temporal search range
    * @param  area      The spatial search range (given as a multipolygon)
    * @param  divide    A function that takes a sequence of (key, tile) pairs a divides it into appropriate temporally-contiguous divisions
    * @param  narrower  A function that turns an area (a tile) into a dictionary of variables to their values, e.g. Map("tasmin" -> 1, "tasmax" -> 3).
    * @param  box       A function that turns a sequence of dictionaries into a sequence of scalers
    * @return           A map from dates to sequences of doubles.  The dates mark the beginnings of temporal divisions and the sequences of one or more scalers are the values derived from the respective chunks.
    */
  def query(
    startTime: ZonedDateTime, endTime: ZonedDateTime, area: MultiPolygon,
    divide: Seq[KV] => Map[ZonedDateTime, Seq[KV]],
    narrower: MultibandTile => Dictionary,
    box: Seq[Dictionary] => Seq[Double]
  ): Map[ZonedDateTime, Seq[Double]] = {
    val collection = dataset
      .query[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]](id)
      .where(Intersects(area))
      .where(Between(startTime, endTime))
      .result.mask(area)

    divide(collection)
      .map({ t => (t._1, t._2.map({ kv => narrower(kv._2) })) })
      .map({ t => (t._1, box(t._2)) })
      .toMap
  }

  def main(args: Array[String]) : Unit = {
    val startTime = ZonedDateTime.of(2018, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)
    val endTime = ZonedDateTime.of(2019, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)
    val collection = query(
      startTime, endTime, polygon,
      Dividers.divideByCalendarMonth,
      Narrowers.byMean,
      Boxen.maxTasmax)

    println(collection)
  }

}
