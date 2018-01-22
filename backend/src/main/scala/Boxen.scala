package com.azavea.climate

import Operations.KV
import Operations.Dictionary
import Operations.TimedDictionary


object Boxen {

  def count(predicate: TimedDictionary => Boolean)(dictionaries: Seq[TimedDictionary]): Seq[Double] = {
    List(
      dictionaries
        .filter(predicate)
        .length.toDouble
    )
  }

  def average(predicate: TimedDictionary => Boolean, variable: String)(dictionaries: Seq[TimedDictionary]): Seq[Double] = {
    val xs =
      dictionaries
        .filter(predicate)
        .map({ case (zdt, d) => d.getOrElse(variable, throw new Exception("No such variable")) })
    List(xs.reduce(_ + _) / xs.length)
  }

  def maximum(predicate: TimedDictionary => Boolean, variable: String)(dictionaries: Seq[TimedDictionary]): Seq[Double] = {
    List(
      dictionaries
        .filter(predicate)
        .map({ case (zdt, d) => d.getOrElse(variable, throw new Exception("No such variable")) })
        .reduce({ (x: Double, y: Double) => if (x >= y) x; else y })
    )
  }

  def minimum(predicate: TimedDictionary => Boolean, variable: String)(dictionaries: Seq[TimedDictionary]): Seq[Double] = {
    List(
      dictionaries
        .filter(predicate)
        .map({ case (zdt, d) => d.getOrElse(variable, throw new Exception("No such variable")) })
        .reduce({ (x: Double, y: Double) => if (x <= y) x; else y })
    )
  }

  def percentile(predicate: TimedDictionary => Boolean, _baseline: Option[String], variable: String)(dictionaries: Seq[TimedDictionary]): Seq[Double] = {
    val p = _baseline match {
      case Some(s) => (s.toDouble)/100.0
      case None => 0.50
    }
    val xs = dictionaries
      .map({ case (zdt, d) => d.getOrElse(variable, throw new Exception("No such variable")) })
      .sorted
      .toArray
    val index = Math.round(math.min(xs.length * p, xs.length-1)).toInt

    List(xs(index))
  }

  def total(predicate: TimedDictionary => Boolean, variable: String)(dictionaries: Seq[TimedDictionary]): Seq[Double] = {
    List(
      dictionaries
        .filter(predicate)
        .map({ case (zdt, d) => d.getOrElse(variable, throw new Exception("No such variable")) })
        .sum
    )
  }

  private def spans[X](xs: Seq[X], pred: X => Boolean): Seq[Seq[X]] = {
    if (xs.length == 0)
      return List()
    else if (!pred(xs.head))
      return spans(xs.drop(1),pred)
    else {
      val (a, b)  = xs.span(pred)
      return List(a) ++ spans(b, pred)
    }
  }

  def countStreaks(predicate: TimedDictionary => Boolean, _baseline: Option[String])(dictionaries: Seq[TimedDictionary]): Seq[Double] = {
    val baseline = _baseline match {
      case Some(s) => s.toDouble
      case None => 0.0
    }

    List(
      spans(dictionaries, predicate)
        .map(_.length)
        .filter(_ >= baseline)
        .length
        .toDouble
    )
  }

  def maxStreak(predicate: TimedDictionary => Boolean)(dictionaries: Seq[TimedDictionary]): Seq[Double] = {
    List(
      spans(dictionaries, predicate)
        .map(_.length)
        .reduce({ (a: Int, b: Int) => if (a > b) a; else b })
        .toDouble
    )
  }

  def diurnalTemperatureRange(predicate: TimedDictionary => Boolean)(dictionaries: Seq[TimedDictionary]): Seq[Double] = {
    val xs =
      dictionaries
        .filter(predicate)
        .map({ case (zdt, d) =>
          val tasmax = d.getOrElse("tasmax", throw new Exception("No such variable"))
          val tasmin = d.getOrElse("tasmin", throw new Exception("No such variable"))
          tasmax - tasmin
        })
    List(xs.sum / xs.length)
  }

  def degreeDays(predicate: TimedDictionary => Boolean, _baseline: Option[String])(dictionaries: Seq[TimedDictionary]): Seq[Double] = {
    val baseline = _baseline match {
      case Some(s) => s.toDouble
      case None => 0.0
    }

    List(
      dictionaries
        .filter(predicate)
        .map({ case (zdt, d) =>
          val tasmax = d.getOrElse("tasmax", throw new Exception("No such variable"))
          val tasmin = d.getOrElse("tasmin", throw new Exception("No such variable"))
          baseline - (tasmax+tasmin)/2.0
        })
        .sum
    )
  }

  def accumulatedFreezingDegreeDays(predicate: TimedDictionary => Boolean)(dictionaries: Seq[TimedDictionary]): Seq[Double] = {
    List(
      dictionaries
        .filter(predicate)
        .map({ case (zdt, d) =>
          val tasmax = d.getOrElse("tasmax", throw new Exception("No such variable"))
          val tasmin = d.getOrElse("tasmin", throw new Exception("No such variable"))
          273.15 - (tasmax + tasmin)/2.0
        })
        .scanLeft(0.0)(_ + _)
        .reduce({ (a: Double, b: Double) => math.max(a,b) })
    )
  }

  /* ------------------------------------------------------------------------ */

  def maxTasmin(dictionaries: Seq[TimedDictionary]): Seq[Double] = {
    List(
      dictionaries
        .map({ case (_, d) => d.getOrElse("tasmin", throw new Exception) })
        .reduce({ (a, x) => math.max(a, x) })
    )
  }

}
