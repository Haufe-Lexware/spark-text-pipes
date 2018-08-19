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

import com.haufe.umantis.ds.nlp.params._
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.ScalaReflection.schemaFor
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}


class SimilarityScorerDenseVector(override val uid: String)
  extends Transformer with HasInputCol with HasOutputCol
    with HasBaseVectorCol with ValidateColumnSchema {

  def this() = this(Identifiable.randomUID("SimilarityScorerDenseVector"))

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  private val vectorUDT = ScalaReflection.schemaFor[DenseVector].dataType

  def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)

    val calculateSimilarity = udf {
      (v0: DenseVector, v1: DenseVector) => {
        val size = v0.size
        val base = v0.values
        val vector = v1.values
        var i = 0
        var dotProduct = 0.0
        while (i < size) {
          dotProduct += base(i) * vector(i)
          i += 1
        }
        dotProduct.toFloat
      }
    }

    dataset.withColumn($(outputCol), calculateSimilarity(col($(baseVectorCol)), col($(inputCol))))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateColumnSchema($(inputCol), vectorUDT, schema)
    validateColumnSchema($(baseVectorCol), vectorUDT, schema)

    if (schema.fieldNames.contains($(outputCol))) {
      throw new IllegalArgumentException(s"Output column ${$(outputCol)} already exists.")
    }
    val outputFields = schema.fields :+
      StructField($(outputCol), schemaFor[Float].dataType, nullable = false)
    StructType(outputFields)
  }

  override def copy(extra: ParamMap): SimilarityScorerDenseVector =
    defaultCopy[SimilarityScorerDenseVector](extra)

}