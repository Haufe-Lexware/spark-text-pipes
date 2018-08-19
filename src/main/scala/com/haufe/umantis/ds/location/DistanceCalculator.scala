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

package com.haufe.umantis.ds.location

import com.haufe.umantis.ds.nlp.params.{HasBaseLocationCol, ValidateColumnSchema}
import org.apache.spark.internal.Logging
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.catalyst.ScalaReflection.schemaFor
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.util.Try


class DistanceCalculator(override val uid: String)
  extends Transformer
    with HasInputCol with HasOutputCol with HasBaseLocationCol with Logging
    with ValidateColumnSchema {

  implicit def rowToGeoCoordinates(row: Row): GeoCoordinates =
    GeoCoordinates(row.getDouble(0), row.getDouble(1))

  def this() = this(Identifiable.randomUID("DistanceCalculator"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)

    val calculateDistance = udf({

      (baseLocation: Row, locations: Seq[Row]) /* also coordinates */ => {

        locations.map(location =>
          Try(
            Location.getDistance[GeoCoordinates](baseLocation, location)
          ).getOrElse(100000f)
        )
          .min
      }
    })

    dataset.withColumn($(outputCol), calculateDistance(col($(inputCol)), col($(baseLocationCol))))
  }

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)


  override def transformSchema(schema: StructType): StructType = {
    validateColumnSchema($(baseLocationCol), schemaFor[Array[GeoCoordinates]].dataType, schema)
    validateColumnSchema($(inputCol), schemaFor[GeoCoordinates].dataType, schema)

    if (schema.fieldNames.contains($(outputCol))) {
      throw new IllegalArgumentException(s"Output column ${$(outputCol)} already exists.")
    }
    val outputFields = schema.fields :+
      StructField($(outputCol), schemaFor[Float].dataType, nullable = false)
    StructType(outputFields)
  }

  override def copy(extra: ParamMap): DistanceCalculator = defaultCopy[DistanceCalculator](extra)
}
