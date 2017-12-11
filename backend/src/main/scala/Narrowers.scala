package com.azavea.climate

import geotrellis.raster._

import Operations.KV
import Operations.Dictionary


object Narrowers {

  /**
    * An example of the `narrower` lambda from the `query` function.
    * Converts an area (a tile) into a dictionary of the form
    * Map("tasmin" -> x, "tasmax" -> y, "pr" -> z).
    */
  def byMean(area: MultibandTile): Dictionary = {
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

    Map(
      "tasmin" -> tasmin,
      "tasmax" -> tasmax,
      "pr" -> Double.NaN // XXX
    )
  }

}
