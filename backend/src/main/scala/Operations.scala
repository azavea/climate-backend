package com.azavea.climate

import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.s3._
import geotrellis.vector._
import geotrellis.vector.io._

import java.time.{ ZonedDateTime, ZoneId, ZoneOffset }


object Operations {

  type KV = (SpaceTimeKey, MultibandTile)
  type Dictionary = Map[String, Double]

  val bucket = "ingested-gddp-data"
  val prefix = "rcp85_r1i1p1_CanESM2"
  val as = S3AttributeStore(bucket, prefix)
  val id = LayerId("rcp85_r1i1p1_CanESM2_benchmark_64_years_temp", 0)
  val dataset = S3CollectionLayerReader(as)

  def divideByCalendarMonth(collection: Seq[KV]): Map[ZonedDateTime, Seq[KV]] = {
    collection.groupBy({ kv =>
      val time = kv._1.time
      val year: Int = time.getYear
      val month: Int = time.getMonth.getValue
      val zone: ZoneId = time.getZone
      ZonedDateTime.of(year, month, 1, 0, 0, 0, 0, zone)
    })
  }

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

  def main(args: Array[String]) : Unit = {

    val geojsonUri = "./geojson/Los_Angeles.geo.json"
    val polygon =
      scala.io.Source.fromFile(geojsonUri, "UTF-8")
        .getLines
        .mkString
        .extractGeometries[MultiPolygon]
        .head
    val polygonExtent = polygon.envelope

    val col = dataset
      .query[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]](id)
      .where(Intersects(polygon))
      .where(Between(
        ZonedDateTime.of(2019, 12, 22, 0, 0, 0, 0, ZoneOffset.UTC),
        ZonedDateTime.of(2020, 6, 21, 0, 0, 0, 0, ZoneOffset.UTC)))
      .result
      .mask(polygon)

    println(
      divideByCalendarMonth(col)
        .map({ t => (t._1, t._2.map({ kv => areaToTasminTasmax(kv._2) })) })
    )
  }

}
