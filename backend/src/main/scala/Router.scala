package com.azavea.climate

import geotrellis.vector._
import geotrellis.vector.io._

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.server.Directives._
import ch.megard.akka.http.cors.scaladsl.CorsDirectives._
import ch.megard.akka.http.cors.scaladsl.settings._

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

import java.time.format.DateTimeFormatter
import java.time.{ZonedDateTime, ZoneOffset}

import spray.json._
import spray.json.DefaultJsonProtocol._


object Router {

  val dateTimeFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")

  val settings =
    CorsSettings.defaultSettings.copy(
      allowedMethods = scala.collection.immutable.Seq(GET, POST, PUT, HEAD, OPTIONS, DELETE)
    )

  def vanilla(startTime: ZonedDateTime, endTime: ZonedDateTime, area: MultiPolygon) = {
    Operations.futureQuery(
      startTime, endTime, area,
      Dividers.divideByCalendarMonth,
      Narrowers.byMean,
      Boxen.maxTasmax
    ).map({ f =>
      f.map({ case (_k, _v) =>
        val k = _k.format(dateTimeFormat)
        val v = _v.toList
        k -> v
      }).toJson
    })
  }

  def queryRoute =
    parameter("startTime", "endTime") { (_startTime, _endTime) =>
      val startTime  = ZonedDateTime.parse(_startTime, dateTimeFormat)
      val endTime = ZonedDateTime.parse(_endTime, dateTimeFormat)
      pathEndOrSingleSlash {
        post {
          entity(as[String]) { json =>
            val area: MultiPolygon = json.extractGeometries[MultiPolygon].head
            complete { vanilla(startTime, endTime, area) }
          }
        }
      }
    }

  def routes() =
    cors(settings) {
      pathPrefix("query") { queryRoute }
    }

}
