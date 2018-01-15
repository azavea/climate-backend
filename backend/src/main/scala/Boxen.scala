package com.azavea.climate

import Operations.KV
import Operations.Dictionary


object Boxen {

  /**
    * An example of the `box` lambda from the `query` function.  It
    * takes a sequence of dictionaries (a temporal division) and
    * produces a sequence of (one element lists of) scalers.
    */
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

}
