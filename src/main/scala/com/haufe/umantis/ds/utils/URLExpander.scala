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

package com.haufe.umantis.ds.utils

import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.catalyst.ScalaReflection.schemaFor
import org.apache.spark.sql.types.{StructField, StructType}
import com.haufe.umantis.ds.nlp.params.ValidateColumnSchema
import org.apache.spark.ml.param.ParamMap


class URLExpander(override val uid: String)
  extends Transformer with HasInputCol with HasOutputCol with ValidateColumnSchema {

  def this() = this(Identifiable.randomUID("URLExpander"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    val result = dataset.toDF().rdd.mapPartitions(iter => {
      val expander: URLUnshortener = URLUnshortener()

      iter.map(r => {
        val urls = r.getAs[Seq[String]]($(inputCol))
        val expandedUrls = urls.map(url => {
          val res = expander.expand(url)
          Row(res.origUrl, res.finalUrl, res.connects, res.numRedirects)
        })

        Row.fromSeq(r.toSeq :+ expandedUrls)
      })
    })

    dataset.sparkSession.createDataFrame(result, transformSchema(dataset.schema))
  }

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)


  override def transformSchema(schema: StructType): StructType = {
    validateColumnSchema($(inputCol), schemaFor[Seq[String]].dataType, schema)

    if (schema.fieldNames.contains($(outputCol))) {
      throw new IllegalArgumentException(s"Output column ${$(outputCol)} already exists.")
    }
    val outputFields = schema.fields :+
      StructField($(outputCol), schemaFor[Array[CheckedURL]].dataType, nullable = false)

    StructType(outputFields)
  }

  override def copy(extra: ParamMap): URLExpander = defaultCopy[URLExpander](extra)
}
