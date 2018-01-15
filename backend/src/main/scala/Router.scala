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
      Boxen.maxTasmin
    ).map({ f =>
      f.map({ case (_k, _v) =>
        val k = _k.format(dateTimeFormat)
        val v = _v.toList
        k -> v
      }).toJson
    })
  }

  def arrayIndicator = {
    parameter("box", "startTime", "endTime", "divider" ?) { (_box, _startTime, _endTime, _divider) =>
      val box: Seq[Operations.Dictionary] => Seq[Double] = _box match {
        case "averageTasmax" => Boxen.averageTasmax
        case _ => Boxen.maxTasmin
      }
      val startTime  = ZonedDateTime.parse(_startTime)
      val endTime = ZonedDateTime.parse(_endTime)
      val divider: Seq[Operations.KV] => Map[ZonedDateTime, Seq[Operations.KV]] = _divider match {
        case Some("month") => Dividers.divideByCalendarMonth
        case Some("year") => Dividers.divideByCalendarYear
        case _ => Dividers.divideByInfinity
      }

      pathEndOrSingleSlash {
        post {
          entity(as[String]) { json =>
            val area: MultiPolygon = json.extractGeometries[MultiPolygon].head

            complete {
              Operations.futureQuery(
                startTime, endTime, area,
                divider,
                Narrowers.byMean,
                box
              ).map({ f =>
                f.map({ case (_k, _v) =>
                  val k = _k.format(dateTimeFormat)
                  val v = _v.toList
                  k -> v
                }).toJson
              })
            }

          }
        }
      }
    }
  }

  def routes() =
    cors(settings) {
      pathPrefix("arrayIndicator") { arrayIndicator }
    }

}
