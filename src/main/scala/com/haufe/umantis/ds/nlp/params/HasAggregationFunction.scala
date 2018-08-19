/**
  * This program is free software: you can redistribute it and/or modify
  * it under the terms of the GNU General Public License as published by
  * the Free Software Foundation, either version 3 of the License, or
  * (at your option) any later version.
  *
  * This program is distributed in the hope that it will be useful,
  * but WITHOUT ANY WARRANTY; without even the implied warranty of
  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  * GNU General Public License for more details.
  *
  * You should have received a copy of the GNU General Public License
  * along with this program.  If not, see <https://www.gnu.org/licenses/>.
  */

package com.haufe.umantis.ds.nlp.params

import org.apache.spark.ml.param.{Param, Params}


/**
  * Trait for shared param similarity function. Holding similarity function name
  *
  */
trait HasAggregationFunction extends Params {

  /**
    * Param for aggregation function.
    * @group param
    */
  final val aggregationFunction: Param[Seq[Float] => Float] =
    new Param[Seq[Float] => Float](
      this,
      "aggregationFunction",
      "the aggregation function")

  /** @group setParam */
  final def setAggregationFunction(value: Seq[Float] => Float): this.type = {
    set(aggregationFunction, value)
  }

  /** @group getParam */
  final def getAggregationFunction: Seq[Float] => Float = $(aggregationFunction)
}
