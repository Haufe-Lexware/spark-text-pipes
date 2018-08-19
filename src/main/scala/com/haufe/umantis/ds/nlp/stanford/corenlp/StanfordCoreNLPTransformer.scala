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

package com.haufe.umantis.ds.nlp.stanford.corenlp

import com.haufe.umantis.ds.nlp.TransformerList
import com.haufe.umantis.ds.nlp.params.{HasDefaultLanguage, HasLanguageCol}
import com.haufe.umantis.ds.spark.SparkSessionWrapper
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.HasInputCol
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.catalyst.ScalaReflection.schemaFor
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

class StanfordCoreNLPTransformer(
                                  private val pipelines: StanfordPipelines,
                                  private val
                                  bStanfordCoreNLPHelper: Broadcast[StanfordCoreNLPHelper],
                                  override val uid: String
                                )
  extends Transformer with HasInputCol with HasLanguageCol with HasDefaultLanguage with
    TransformerList {

  /** @group setParam */
  def setInputCol(value: String): StanfordCoreNLPTransformer =
    set(inputCol, value).asInstanceOf[StanfordCoreNLPTransformer]

  override def transform(data: Seq[Map[String, Any]]): Seq[Map[String, Any]] = {
    val annotators = pipelines.pipelines.transform((lang, pipeline) => bStanfordCoreNLPHelper
      .value.annotationPipeline(pipeline, lang))
    data.map(
      p => {
        val document = p($(inputCol)).asInstanceOf[String]
        val language = p($(languageCol)).asInstanceOf[String]

        val pipeline = annotators.getOrElse(language, annotators($(defaultLanguage)))

        p ++ pipelines.columns.zip(pipeline.annotate(document)).map { case (a, b) => a -> b }
      })
  }

  private def transformMAPPAR(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    val result = dataset.toDF().rdd.mapPartitions(iter => {
      val annotators = pipelines.pipelines.transform((lang, pipeline) => bStanfordCoreNLPHelper
        .value.annotationPipeline(pipeline, lang))
      iter.map(r => {
        val document = r.getAs[String]($(inputCol))
        val language = r.getAs[String]($(languageCol))
        val pipeline = annotators.getOrElse(language, annotators($(defaultLanguage)))

        val res = r.toSeq ++ pipeline.annotate(document)
        Row.fromSeq(res)
      })
    })

    dataset.sparkSession.createDataFrame(result, transformSchema(dataset.schema))
  }

  private def transformUDF(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)

    val annotate = udf({
      val annotators = pipelines.pipelines.transform((lang, pipeline) => bStanfordCoreNLPHelper
        .value.annotationPipeline(pipeline, lang))

      (language: String, document: String) => {
        val pipeline = annotators.getOrElse(language, annotators($(defaultLanguage)))
        pipeline.annotate(document)
      }
    })

    val tmpColName = $(inputCol) + "_tmp"
    val transCols = pipelines.columns.zipWithIndex.map { case (c, i) => col(tmpColName)(i).as(c) }
    val outputColumns = dataset.columns.map(col) ++ transCols
    dataset
      .withColumn(tmpColName, annotate(col($(languageCol)), col($(inputCol))))
      .select(outputColumns: _*)
  }

  private var transformFunction: Dataset[_] => DataFrame = transformMAPPAR: Dataset[_] => DataFrame

  def setTransformFunction(f: Dataset[_] => DataFrame): Unit = {
    transformFunction = f
  }

  override def transform(dataset: Dataset[_]): DataFrame = transformFunction(dataset)

  protected def validateInputType(inputType: DataType): Unit = {
    require(inputType == schemaFor[String].dataType)
  }

  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
    validateInputType(inputType)
    if (pipelines.columns.exists(schema.fieldNames.contains(_))) {
      val invalidOutputColumns = pipelines.columns.filter(schema.fieldNames.contains(_))
      throw new IllegalArgumentException(s"Output columns $invalidOutputColumns already exists.")
    }

    val outputFields = schema.fields ++ pipelines.columns.map(StructField(_,
      schemaFor[Seq[String]].dataType, nullable = true))
    StructType(outputFields)
  }

  override def copy(extra: ParamMap): StanfordCoreNLPTransformer = {
    val to = StanfordCoreNLPTransformer(pipelines, Some(uid))
    copyValues(to, extra)
  }

  def this(pipelines: StanfordPipelines, bSnlp: Broadcast[StanfordCoreNLPHelper]) =
    this(pipelines, bSnlp, Identifiable.randomUID("StanfordCoreNLPTransformer"))
}

object StanfordCoreNLPTransformer extends SparkSessionWrapper {

  def apply(pipelines: StanfordPipelines, uid: Option[String] = None)
  : StanfordCoreNLPTransformer = {
    uid match {
      case Some(definedUid) => new StanfordCoreNLPTransformer(pipelines, StanfordCoreNLPHelper(),
        definedUid)
      case _ => new StanfordCoreNLPTransformer(pipelines, StanfordCoreNLPHelper())
    }
  }
}
