package com.azavea.climate

import Operations.KV
import Operations.Dictionary


object Boxen {

  def maxTasmin(dictionaries: Seq[Dictionary]): Seq[Double] = {
    List(
      dictionaries
        .map({ d => d.getOrElse("tasmin", throw new Exception) })
        .reduce({ (a, x) =>
          if (x.isNaN && !a.isNaN) a
          else if (!x.isNaN && a.isNaN) x
          else math.max(a, x)
        })
    )
  }

  def averageTasmax(dictionaries: Seq[Dictionary]): Seq[Double] = {
    val tasmaxen = dictionaries
      .map({ d => d.getOrElse("tasmax", throw new Exception) })
      .filter({ t => !t.isNaN })
    List(tasmaxen.sum / tasmaxen.length)
  }

  def extremePrecipitationEvents(baseline: Double)(dictionaries: Seq[Dictionary]): Seq[Double] = {
    List(
      dictionaries
        .map({ d => d.getOrElse("pr", throw new Exception) })
        .filter({ pr => pr > baseline })
        .length
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

  def heatWaveDurationIndex(baseline: Double)(dictionaries: Seq[Dictionary]): Seq[Double] = {
    val ts = dictionaries.map({ d => d.getOrElse("tasmax", throw new Exception) })

    spans(ts, { temp: Double => temp > baseline }).map(_.length.toDouble)
  }

}
