package com.azavea.climate

import geotrellis.raster._

import Operations.KV
import Operations.Dictionary


object Narrowers {

  /**
    * An example of the `narrower` lambda from the `query` function.
    * Converts an area (some tiles) into a dictionary of the form
    * Map("tasmin" -> x, "tasmax" -> y, "pr" -> z).
    */
  def byMean(area: Seq[MultibandTile]): Dictionary = {
    var count1: Int = 0
    var count2: Int = 0
    var count3: Int = 0
    var tasmin: Double = 0.0
    var tasmax: Double = 0.0
    var pr: Double = 0.0

    area.foreach({ tile =>
      tile.band(0).foreachDouble({ z: Double =>
        if (!isNoData(z)) {
          count1 = count1 + 1
          tasmin = tasmin + z
        }
      })

      tile.band(1).foreachDouble({ z: Double =>
        if (!isNoData(z)) {
          count2 = count2 + 1
          tasmax = tasmax + z
        }
      })

      tile.band(2).foreachDouble({ z: Double =>
        if (!isNoData(z)) {
          count3 = count3 + 1
          pr = pr + z
        }
      })
    })

    tasmin /= count1
    tasmax /= count2
    pr /= count3

    Map(
      "tasmin" -> tasmin,
      "tasmax" -> tasmax,
      "tasavg" -> (tasmin+tasmax)/2.0,
      "pr" -> pr
    )
  }

}
