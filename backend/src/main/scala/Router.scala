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

import Operations.KV
import Operations.Dictionary
import Operations.TimedDictionary


object Router {

  val dateTimeFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")

  val settings =
    CorsSettings.defaultSettings.copy(
      allowedMethods = scala.collection.immutable.Seq(GET, POST, PUT, HEAD, OPTIONS, DELETE)
    )

  def getPredicate2(_predicate: Option[String]): Double => TimedDictionary => Boolean = {
    _predicate match {
      case Some(predicate) =>
        val operation =
          if (predicate contains ">=") ">="
          else if (predicate contains "<=") "<="
          else if (predicate contains ">") ">"
          else if (predicate contains "<") "<"
          else throw new Exception("No such operation")
        val variable = predicate.split(operation).head.trim.toLowerCase match {
          case "tasmin" => "tasmin"
          case "tasmax" => "tasmax"
          case "pr" => "pr"
          case _ => throw new Exception("No such variable")
        }
        { y: Double => td: TimedDictionary =>
          val x = td._2.getOrElse(variable, throw new Exception)
          operation match {
            case ">=" => x >= y
            case "<=" => x <= y
            case ">" => x > y
            case "<" => x < y
            case _ => throw new Exception("No such operation (you shouldn't be seeing this)")
          }
        }
      case None =>
        { _: Double => _: TimedDictionary => true }
    }
  }

  def getPredicate1(_predicate: Option[String]): TimedDictionary => Boolean = {
    _predicate match {
      case Some(predicate) =>
        val operation =
          if (predicate contains ">=") ">="
          else if (predicate contains "<=") "<="
          else if (predicate contains ">") ">"
          else if (predicate contains "<") "<"
          else throw new Exception("No such operation")
        val y = predicate.split(operation).drop(1).head.trim.toDouble
        getPredicate2(_predicate)(y)
      case None => { td: TimedDictionary => true }
    }
  }

  def getVariable(_predicate: Option[String], _variable: Option[String]): String = {
    _predicate match {
      case Some(predicate) =>
        val operation =
          if (predicate contains ">=") ">="
          else if (predicate contains "<=") "<="
          else if (predicate contains ">") ">"
          else if (predicate contains "<") "<"
          else throw new Exception("No such operation")
        predicate.split(operation).head.trim.toLowerCase match {
          case "tasmin" => "tasmin"
          case "tasmax" => "tasmax"
          case "pr" => "pr"
          case _ => throw new Exception("No such variable")
        }
      case None =>
        _variable match {
          case Some(variable) => variable
          case _ => throw new Exception("No variable")
        }
    }
  }

  // ---------------------------------

  def indicator = {
    parameter("operation", "startTime", "endTime", "divisions", "predicate" ?, "variable" ?) { (_operation, _startTime, _endTime, _divisions, _predicate, _variable) =>
      val startTime  = ZonedDateTime.parse(_startTime)
      val endTime = ZonedDateTime.parse(_endTime)
      val division: Seq[KV] => Map[ZonedDateTime, Seq[KV]] = _divisions match {
        case "month" => Dividers.divideByCalendarMonth
        case "year" => Dividers.divideByCalendarYear
        case "infinity" => Dividers.divideByInfinity
      }
      lazy val predicate1: TimedDictionary => Boolean = getPredicate1(_predicate)
      lazy val predicate2: Double => TimedDictionary => Boolean = getPredicate2(_predicate)
      lazy val variable: String = getVariable(_predicate, _variable)
      val box: Seq[TimedDictionary] => Seq[Double] = _operation match {
        case "maxTemperatureThreshold" | "minTemperatureThreshold" | "precipitationThreshold" => Boxen.count(predicate1)
        case "averageHighTemperature" => Boxen.average(predicate1, "tasmax")
        case "averageLowTemperature" => Boxen.average(predicate1, "tasmin")
        case "maxHighTemperature" => Boxen.maximum(predicate1, "tasmax")
        case "minLowTemperature" => Boxen.minimum(predicate1, "tasmin")
      }

      pathEndOrSingleSlash {
        post {
          entity(as[String]) { json =>
            val area: MultiPolygon = json.extractGeometries[MultiPolygon].head

            complete {
              Operations.futureQuery(
                startTime, endTime,
                area,
                division,
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

          }}}

    }
  }

  // ---------------------------------
  val rng = scala.util.Random

  def s3month = {
      val startTime  = ZonedDateTime.parse("2018-01-01T00:00:00Z").plusMonths(rng.nextInt(70*12))
      val endTime = startTime.plusMonths(1)

      pathEndOrSingleSlash {
        post {
          entity(as[String]) { json =>
            val area: MultiPolygon = json.extractGeometries[MultiPolygon].head

            complete {
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

          }}}
  }

  def s3year = {
      val startTime  = ZonedDateTime.parse("2018-01-01T00:00:00Z").plusMonths(rng.nextInt(70*12))
      val endTime = startTime.plusYears(1)

      pathEndOrSingleSlash {
        post {
          entity(as[String]) { json =>
            val area: MultiPolygon = json.extractGeometries[MultiPolygon].head

            complete {
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

          }}}
  }


  def s3decade = {
      val startTime  = ZonedDateTime.parse("2018-01-01T00:00:00Z").plusMonths(rng.nextInt(70*12))
      val endTime = startTime.plusYears(10)

      pathEndOrSingleSlash {
        post {
          entity(as[String]) { json =>
            val area: MultiPolygon = json.extractGeometries[MultiPolygon].head

            complete {
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

          }}}
  }

  // ---------------------------------

  def routes() =
    cors(settings) {
      pathPrefix("indicator") { indicator } ~
      pathPrefix("s3month") { s3month } ~ pathPrefix("s3year") { s3month } ~ pathPrefix("s3decade") { s3decade }
    }

}
