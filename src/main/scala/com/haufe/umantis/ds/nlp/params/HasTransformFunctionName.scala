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
import org.apache.spark.sql.{DataFrame, Dataset}


/**
  * Trait for shared param transform function.
  */
trait HasTransformFunctionName extends Params {

  protected val transformFunctions: Map[String,Dataset[_] => DataFrame]
  /**
    * Param for transform function.
    * @group param
    */
  final val transformFunction: Param[String] =
    new Param[String](
      this,
      "transformFunction",
      "the transform function name")

  /** @group setParam */
  final def setTransformFunction(value: String): this.type = {
    set(transformFunction, value)
  }
  /** @group getParam */
  final def getTransformFunction: Dataset[_] => DataFrame = transformFunctions($(transformFunction))

  def validTransformFunctionNames:Iterable[String] = transformFunctions.keys

  def transform(dataset: Dataset[_]): DataFrame = getTransformFunction(dataset)
}
