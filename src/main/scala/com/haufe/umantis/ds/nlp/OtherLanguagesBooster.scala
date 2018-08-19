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
import org.apache.spark.internal.Logging
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.catalyst.ScalaReflection.schemaFor
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}


class OtherLanguagesBooster(override val uid: String)
  extends Transformer with HasInputCol with HasOutputCol with HasLanguageCol with Logging
    with HasBaseLanguageCol with ValidateColumnSchema {

  def this() = this(Identifiable.randomUID("OtherLanguagesBoost"))

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  val boostsMap: Param[Map[String, Float]] =
    new Param[Map[String, Float]](this, "boostsMap",
      "how much to boost other languages given the base one.")

  def setBoostsMap(value: Map[String, Float]): this.type = set(boostsMap, value)

  def getBoostsMap: Map[String, Float] = $(boostsMap)

  setDefault(boostsMap, Map("en" -> 1.3f, "de" -> 1.17f))

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)

    val boostOtherLanguage = udf {

      (language: String, baseLang: String, similarity: Float) =>
        if (language != baseLang)
          similarity * $(boostsMap).getOrElse(baseLang, 1.0f)
        else
          similarity
    }

    dataset.withColumn($(outputCol), boostOtherLanguage(col($(languageCol)),
      col($(baseLanguageCol)), col($(inputCol))))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateColumnSchema($(inputCol), schemaFor[Float].dataType, schema)
    validateColumnSchema($(languageCol), schemaFor[String].dataType, schema)
    validateColumnSchema($(baseLanguageCol), schemaFor[String].dataType, schema)

    if (schema.fieldNames.contains($(outputCol))) {
      throw new IllegalArgumentException(s"Output column ${$(outputCol)} already exists.")
    }
    val outputFields = schema.fields :+
      StructField($(outputCol), schemaFor[Float].dataType, nullable = false)
    StructType(outputFields)
  }

  override def copy(extra: ParamMap): OtherLanguagesBooster =
    defaultCopy[OtherLanguagesBooster](extra)
}
