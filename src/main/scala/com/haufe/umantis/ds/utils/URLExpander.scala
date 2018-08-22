package com.haufe.umantis.ds.utils

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection.schemaFor
import org.apache.spark.sql.types.DataType


class URLExpander(override val uid: String)
  // first arg is input, second output, third is class name
  extends UnaryTransformer[Seq[String], Seq[String], URLExpander] {

  def this() = this(Identifiable.randomUID("URLExpander"))

  val expander: URLUnshortener = URLUnshortener()
  val expanderB: Broadcast[URLUnshortener] =
    SparkSession.builder().getOrCreate().sparkContext.broadcast(expander)

  def expandURLs(urls: Seq[String]): Seq[String] = {
    urls.map(url => expanderB.value.expand(url))
  }

  override protected def createTransformFunc: Seq[String] => Seq[String] = expandURLs


  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType == schemaFor[Seq[String]].dataType)
  }

  override protected def outputDataType: DataType = {
    schemaFor[Seq[String]].dataType
  }
}
