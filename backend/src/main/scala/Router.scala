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

  def parsePredicate(_predicate: Option[String]): TimedDictionary => Boolean = {
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
        val y = predicate.split(operation).drop(1).head.trim.toDouble

        // Predicate Function
        { td: TimedDictionary =>
          val x = td._2.getOrElse("variable", throw new Exception)
          operation match {
            case ">=" => x >= y
            case "<=" => x <= y
            case ">" => x > y
            case "<" => x < y
            case _ => throw new Exception("No such operation (you shouldn't be seening this)")
          }
        }
      case None => { td: TimedDictionary => true }
    }
  }

  // ---------------------------------

  // def indicator = {
  //   parameter("indicator", "startTime", "endTime", "divisions", "predicate" ?, "variable" ?) { (_indicator, _startTime, _endTime, _divisions, _baseline, _variable) =>
  //     val startTime  = ZonedDateTime.parse(_startTime)
  //     val endTime = ZonedDateTime.parse(_endTime)
  //     val division: Seq[KV] => Map[ZonedDateTime, Seq[KV]] = _divisions match {
  //       case "month" => Dividers.divideByCalendarMonth
  //       case "year" => Dividers.divideByCalendarYear
  //       case "infinity" => Dividers.divideByInfinity
  //     }
  //     val predicate: Double => Boolean = _baseline match {
  //       case Some(baseline) => { x: Double => x > baseline.toDouble }
  //       case None => { x: Double => true }
  //     }
  //     val box: Seq[TimedDictionary] => Seq[Double] = (_indicator, _baseline, _variable) match {
  //       // case ("minTempertureThreshold", _, _) => Boxen.count(predicate, "tasmin")
  //       // case ("
  //     }

  //     pathEndOrSingleSlash {
  //       post {
  //         entity(as[String]) { json =>
  //           val area: MultiPolygon = json.extractGeometries[MultiPolygon].head

  //           complete {
  //             Operations.futureQuery(
  //               startTime, endTime, area,
  //               division,
  //               Narrowers.byMean,
  //               box
  //             ).map({ f =>
  //               f.map({ case (_k, _v) =>
  //                 val k = _k.format(dateTimeFormat)
  //                 val v = _v.toList
  //                 k -> v
  //               }).toJson
  //             })
  //           }

  //         }}}

  //   }
  // }

  def arrayIndicator = {
    parameter("box" ?, "startTime", "endTime", "divider" ?) { (_box, _startTime, _endTime, _divider) =>
      val box: Seq[TimedDictionary] => Seq[Double] = _box match {
        case Some("averageTasmax") => Boxen.averageTasmax
        case Some("maxTasmin") => Boxen.maxTasmin
        case _ => Boxen.maxTasmin
      }
      val startTime  = ZonedDateTime.parse(_startTime)
      val endTime = ZonedDateTime.parse(_endTime)
      val divider: Seq[KV] => Map[ZonedDateTime, Seq[KV]] = _divider match {
        case Some("month") => Dividers.divideByCalendarMonth
        case Some("year") => Dividers.divideByCalendarYear
        case Some("infinity") => Dividers.divideByInfinity
        case _ => Dividers.divideByCalendarMonth
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

          }}}}
  }

  // ---------------------------------

  def arrayBaselineIndicator = {
    parameter("box", "startTime", "endTime", "divider" ?, "baseline") { (_box, _startTime, _endTime, _divider, _baseline) =>
      val baseline = _baseline.toDouble
      val box: Seq[TimedDictionary] => Seq[Double] = _box match {
        case "extremePrecipitationEvents" => Boxen.extremePrecipitationEvents(baseline)
        case _ => throw new Exception
      }
      val startTime  = ZonedDateTime.parse(_startTime)
      val endTime = ZonedDateTime.parse(_endTime)
      val divider: Seq[KV] => Map[ZonedDateTime, Seq[KV]] = _divider match {
        case Some("month") => Dividers.divideByCalendarMonth
        case Some("year") => Dividers.divideByCalendarYear
        case Some("infinity") => Dividers.divideByInfinity
        case _ => Dividers.divideByCalendarMonth
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

          }}}}
  }

  // ---------------------------------

  def arrayPredicateIndicator = {
    parameter("box", "startTime", "endTime", "divider" ?, "baseline") { (_box, _startTime, _endTime, _divider, _baseline) =>
      val baseline = _baseline.toDouble
      val box: Seq[TimedDictionary] => Seq[Double] = _box match {
        case "heatWaveDurationIndex" => Boxen.heatWaveDurationIndex(baseline)
        case _ => throw new Exception
      }
      val startTime  = ZonedDateTime.parse(_startTime)
      val endTime = ZonedDateTime.parse(_endTime)
      val divider: Seq[KV] => Map[ZonedDateTime, Seq[KV]] = _divider match {
        case Some("month") => Dividers.divideByCalendarMonth
        case Some("year") => Dividers.divideByCalendarYear
        case Some("infinity") => Dividers.divideByInfinity
        case _ => Dividers.divideByCalendarMonth
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

          }}}}
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
      pathPrefix("arrayIndicator") { arrayIndicator } ~
      pathPrefix("arrayBaselineIndicator") { arrayBaselineIndicator } ~
      pathPrefix("arrayPredicateIndicator") { arrayPredicateIndicator } ~
      pathPrefix("s3month") { s3month } ~ pathPrefix("s3year") { s3month } ~ pathPrefix("s3decade") { s3decade }
    }

}
