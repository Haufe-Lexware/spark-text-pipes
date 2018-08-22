package com.haufe.umantis.ds.utils

import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.catalyst.ScalaReflection.schemaFor
import org.apache.spark.sql.types.DataType


class URLValidator(override val uid: String)
// first arg is input, second output, third is class name
  extends UnaryTransformer[Seq[String], Seq[String], URLValidator] {

  def this() = this(Identifiable.randomUID("URLValidator"))

  def validateURLs(urls: Seq[String]): Seq[String] = {
    urls.filter(url => URLIdentifier.isValid(url.toLowerCase))
  }

  override protected def createTransformFunc: Seq[String] => Seq[String] = validateURLs


  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType == schemaFor[Seq[String]].dataType)
  }

  override protected def outputDataType: DataType = {
    schemaFor[Seq[String]].dataType
  }
}
