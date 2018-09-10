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

import com.haufe.umantis.ds.nlp.params.{HasTransliterator, ValidateColumnSchema}
import com.haufe.umantis.ds.utils.EmojiRemoverTransliterator
import com.ibm.icu.text.Transliterator
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection.schemaFor
import org.apache.spark.sql.types.{StructField, StructType}

import scala.collection.mutable


object ICUTransliterators extends Serializable {

  // This is mutable because when used by libraries, users may decide to create new
  // transliterators and they need to add them here
  lazy val additionalTransliterators: mutable.Set[Unit => Transliterator] =
    mutable.Set[Unit => Transliterator](
      {_ => new EmojiRemoverTransliterator()}
    )

  // cached transliterators
  @transient lazy val transliterators: mutable.Map[String, Transliterator] = mutable.Map()

  def getTransformer(transformation: String): Transliterator = {
    transliterators.getOrElseUpdate(transformation, Transliterator.getInstance(transformation))
  }
}

class ICUTransformer(override val uid: String) extends Transformer
  with HasInputCol with HasOutputCol with HasTransliterator with ValidateColumnSchema {

  def this() = this(Identifiable.randomUID("ICUTransformer"))

  setDefault(transliteratorID, "Any-Latin; Latin-ASCII")

  val transliteratorsB: Broadcast[ICUTransliterators.type] =
    SparkSession.builder().getOrCreate().sparkContext.broadcast(ICUTransliterators)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val result = dataset.toDF().rdd.mapPartitions(iter => {

      // Since this is executed remotely and some custom transliterators might not be serializable
      // (e.g. EmojiRemoverTransliterator), we use mapPartitions and register the instances here.
      transliteratorsB
        .value
        .additionalTransliterators
        .foreach(transliteratorCreator => Transliterator.registerInstance(transliteratorCreator()))

      iter.map(r => {
        val text = r.getAs[String]($(inputCol))

        val transliterated = {
          if (text != null) {
            transliteratorsB
              .value
              .getTransformer(getTransliteratorID)
              .transliterate(text)
          } else null
        }

        Row.fromSeq(r.toSeq :+ transliterated)
      })
    })

    dataset.sparkSession.createDataFrame(result, transformSchema(dataset.schema))
  }

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)


  override def transformSchema(schema: StructType): StructType = {
    validateColumnSchema($(inputCol), schemaFor[String].dataType, schema)

    if (schema.fieldNames.contains($(outputCol))) {
      throw new IllegalArgumentException(s"Output column ${$(outputCol)} already exists.")
    }
    val outputFields = schema.fields :+
      StructField($(outputCol), schemaFor[String].dataType, nullable = true)

    StructType(outputFields)
  }

  override def copy(extra: ParamMap): ICUTransformer = defaultCopy[ICUTransformer](extra)
}
