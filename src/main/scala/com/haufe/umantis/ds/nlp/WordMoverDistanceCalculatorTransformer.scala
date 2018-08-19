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

import com.haufe.umantis.ds.nlp.params.{HasBaseDocumentCol, HasBaseLanguageCol, HasLanguageCol,
  ValidateColumnSchema}
import com.haufe.umantis.ds.wmd.{DistanceType, EuclideanDistance, WordMoverDistanceCalculator}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.catalyst.ScalaReflection.schemaFor
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

class WordMoverDistanceCalculatorTransformer(override val uid: String,
                                             calculator: WordMoverDistanceCalculator,
                                             var calculationType: DistanceType = EuclideanDistance)
  extends Transformer
    with HasInputCol with HasLanguageCol
    with HasBaseLanguageCol with HasBaseDocumentCol
    with HasOutputCol with ValidateColumnSchema {


  def this(calculator: WordMoverDistanceCalculator) =
    this(Identifiable.randomUID("WordMoverDistanceTransformer"), calculator)

  def setCalculationType(calculationType: DistanceType): this.type = {
    this.calculationType = calculationType
    this
  }

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)

    val calculateDistance = udf {
      val calc = calculationType
      (document1: Seq[GenericRowWithSchema],
       document2: Seq[GenericRowWithSchema],
       language1: String,
       language2: String) =>

        calculator.computeDistance(document1.map(p => (p.getString(0), p.getInt(1))),
          document2.map(p => (p.getString(0), p.getInt(1))), language1, language2, calc)
    }

    dataset.withColumn($(outputCol), calculateDistance(col($(inputCol)), col($(baseDocumentCol)),
      col($(languageCol)), col($(baseLanguageCol))))

  }

  override def copy(extra: ParamMap): WordMoverDistanceCalculatorTransformer = {
    val copy = new WordMoverDistanceCalculatorTransformer(uid, calculator, calculationType)
    copyValues(copy, extra)
  }

  override def transformSchema(schema: StructType): StructType = {

    validateColumnSchema($(inputCol), schemaFor[Seq[(String, Int)]].dataType, schema)
    validateColumnSchema($(baseDocumentCol), schemaFor[Seq[(String, Int)]].dataType, schema)
    validateColumnSchema($(languageCol), schemaFor[String].dataType, schema)
    validateColumnSchema($(baseLanguageCol), schemaFor[String].dataType, schema)

    if (schema.fieldNames.contains($(outputCol))) {
      throw new IllegalArgumentException(s"Output column ${$(outputCol)} already exists.")
    }
    val outputFields = schema.fields :+
      StructField($(outputCol), schemaFor[Float].dataType, nullable = false)
    StructType(outputFields)
  }

}
