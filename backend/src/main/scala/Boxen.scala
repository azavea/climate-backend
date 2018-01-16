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
    val tasmaxen = dictionaries.map({ d => d.getOrElse("tasmax", throw new Exception) })
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

}
