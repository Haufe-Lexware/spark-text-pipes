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

package com.haufe.umantis.ds.nlp

import com.haufe.umantis.ds.nlp.params.{HasLinearWeightsCol, ValidateColumnSchema}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasInputCols, HasOutputCol}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.catalyst.ScalaReflection.schemaFor
import org.apache.spark.sql.functions.{array, col, udf}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}


class LinearWeigher(override val uid: String)
  extends Transformer
    with HasInputCols with HasOutputCol with HasLinearWeightsCol with ValidateColumnSchema {

  def this() = this(Identifiable.randomUID("LinearWeigher"))

  /** @group setParam */
  def setInputCols(values: Array[String]): this.type = {
    set(inputCols, values)
  }

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)

    val linearCombination = udf {
      (linearWeights:Row, values: Seq[Float]) => {
        val sum = (values, linearWeights.getSeq[Float](1)).zipped.foldLeft((0.0f, 0.0f))((acc, element) => {
          if (element._1 == null)
            acc
          else
            (acc._1 + element._1 * element._2, acc._2 + element._2)
        })
        sum._1 / sum._2
      }
    }
    val inputColumns = array($(inputCols).map(col):_*)
    dataset.withColumn($(outputCol), linearCombination(col($(linearWeightsCol)), inputColumns))
  }

  override def transformSchema(schema: StructType): StructType = {

    $(inputCols).foreach(validateColumnSchema(_,schemaFor[Float].dataType,schema))
    validateColumnSchema($(linearWeightsCol), schemaFor[(Float,Array[Float])].dataType, schema)

    if (schema.fieldNames.contains($(outputCol))) {
      throw new IllegalArgumentException(s"Output column ${$(outputCol)} already exists.")
    }
    val outputFields = schema.fields :+
      StructField($(outputCol), schemaFor[Float].dataType, nullable = false)
    StructType(outputFields)
  }

  override def copy(extra: ParamMap): LinearWeigher = defaultCopy[LinearWeigher](extra)
}
