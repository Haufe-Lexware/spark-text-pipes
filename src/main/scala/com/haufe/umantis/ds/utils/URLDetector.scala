package com.haufe.umantis.ds.utils

import com.linkedin.urls.detection.{UrlDetector, UrlDetectorOptions}
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.catalyst.ScalaReflection.schemaFor
import org.apache.spark.sql.types.DataType

import scala.collection.JavaConverters._


class URLDetector(override val uid: String)
// first arg is input, second output, third is class name
  extends UnaryTransformer[String, Seq[String], URLDetector] {

  def this() = this(Identifiable.randomUID("URLDetector"))

  def detectURLs(text: String): Seq[String] = {

    def doDetectURLs(text: String): Seq[String] = {
      val urls = new UrlDetector(text, UrlDetectorOptions.HTML)
        .detect
        .asScala
        .map(_.normalize)
        .filter(url => {
          val host = url.getHost
          host != null &&
            // this means a '@' was found, we filter emails here
            url.getUsername != "" &&
            // we filter non-existing tlds here
            URLDetector.tlds.contains(host.split('.').takeRight(1)(0))
        })
        .map(_.toString)
      if (urls.nonEmpty) urls else Seq()
    }

    if (text != null) {
      try {
        doDetectURLs(text)
      } catch {
        case _: java.lang.NegativeArraySizeException =>

          // UrlDetector has issues with long sequences of ":" characters
          // (https://github.com/linkedin/URL-Detector/issues/15)
          // and possibly others so in this case we remove them
          // and try again

          val maxSequenceLength = 10
          val replacedText = s"(.)\\1{$maxSequenceLength,}".r.replaceAllIn(text, "")

          try {
            doDetectURLs(replacedText)
          } catch {
            case _: java.lang.NegativeArraySizeException =>
              Seq() // failed again, we give up
          }
      }
    }
    else
      Seq()
  }

  override protected def createTransformFunc: String => Seq[String] = detectURLs


  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType == schemaFor[String].dataType)
  }

  override protected def outputDataType: DataType = {
    schemaFor[Seq[String]].dataType
  }
}
object URLDetector extends ConfigGetter with Serializable {
  val tlds: Set[String] =
    getConfigString("/tld.list")
    .split("\n")
    .toSet
}