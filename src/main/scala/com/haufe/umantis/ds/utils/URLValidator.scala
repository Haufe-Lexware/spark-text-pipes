package com.haufe.umantis.ds.utils

import com.haufe.umantis.ds.nlp.params.ValidateColumnSchema
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.catalyst.ScalaReflection.schemaFor
import org.apache.spark.sql.types.{StructField, StructType}


class URLValidator(override val uid: String)
  extends Transformer with HasInputCol with HasOutputCol with ValidateColumnSchema {

  def this() = this(Identifiable.randomUID("URLValidator"))

  def validateURLs(urls: Seq[String]): Seq[String] = {
    urls.filter(url => URLIdentifier.isValid(url.toLowerCase))
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val result = dataset.toDF().rdd.mapPartitions(iter => {
      iter.map(r => {
        val urls = r.getAs[Seq[String]]($(inputCol))
        val validatedUrls = urls.filter(url => URLIdentifier.isValid(url.toLowerCase))

        Row.fromSeq(r.toSeq :+ validatedUrls)
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
      StructField($(outputCol), schemaFor[Seq[String]].dataType, nullable = false)

    StructType(outputFields)
  }

  override def copy(extra: ParamMap): URLValidator = defaultCopy[URLValidator](extra)
}
