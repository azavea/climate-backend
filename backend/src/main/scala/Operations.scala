package com.azavea.climate

import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.s3._
import geotrellis.vector._
import geotrellis.vector.io._

import java.time.{ZonedDateTime, ZoneOffset}


object Operations {

  def main(args: Array[String]) : Unit = {
    val bucket = "ingested-gddp-data"
    val prefix = "rcp85_r1i1p1_CanESM2"
    val as = S3AttributeStore(bucket, prefix)
    val id = LayerId("rcp85_r1i1p1_CanESM2_benchmark_64_years_temp", 0)

    val geojsonUri = "./geojson/USA.geo.json"
    val polygon =
      scala.io.Source.fromFile(geojsonUri, "UTF-8")
        .getLines
        .mkString
        .extractGeometries[MultiPolygon]
        .head
    val polygonExtent = polygon.envelope

    val reader = S3CollectionLayerReader(as)
    val col = reader
      .query[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]](id)
      .where(Intersects(polygon))
      .where(Between(ZonedDateTime.of(2020, 3, 1, 0, 0, 0, 0, ZoneOffset.UTC), ZonedDateTime.of(2021, 4, 1, 0, 0, 0, 0, ZoneOffset.UTC)))
      .result

    println("XXX " + col.length)
  }

}
