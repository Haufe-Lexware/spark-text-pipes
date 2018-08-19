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

import java.util
import java.util.Properties

import com.haufe.umantis.ds.nlp.LanguagesConfiguration
import com.haufe.umantis.ds.spark.SparkSessionWrapper
import edu.stanford.nlp.io.IOUtils
import edu.stanford.nlp.pipeline._
import edu.stanford.nlp.semgraph.SemanticGraph
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.ScalaReflection.schemaFor
import org.apache.spark.sql.types.StructField

import scala.collection.JavaConverters._


class StanfordCoreNLPHelper(val supportedLanguages: Array[String])
  extends Serializable {
  /**
    * We need to share just 1 pipeline per executor because Stanford CoreNLP
    * models are very memory intensive.
    */
  @transient private lazy val languagePipeline: Map[String, StanfordCoreNLP] = {
    supportedLanguages.map(lang => {
      val props = new Properties()
      props.load(IOUtils.readerFromString(s"StanfordCoreNLP-$lang.properties"))
      lang -> new StanfordCoreNLP(props)
    }).toMap
  }

  def annotationPipeline(pipeline: Seq[StanfordAnnotator[Any]], language: String = "en")
  : StanfordAnnotationPipeline = {
    val configuredAnnotators = languagePipeline(language).pool
    val annotators: Seq[Annotator] = pipeline
      .flatMap(t => t.dependency :+ t.annotator)
      .distinct
      .filterNot("" == _)
      .map(configuredAnnotators.get)

    annotators.foldLeft(new util.HashSet[Any]()) {
      (annotatorsSatisfiedRequirements, annotator) => {
        if (!annotatorsSatisfiedRequirements.containsAll(annotator.requires())) {
          val missing = new util.HashSet[Any](annotator.requires())
          missing.removeAll(annotatorsSatisfiedRequirements)
          throw new IllegalArgumentException(s"Input annotators combination doesn't meet " +
            s"annotator requirements. Pipeline $pipeline and annotator $annotator. Missing " +
            s"requirements $missing.")
        }
        annotatorsSatisfiedRequirements.addAll(annotator.requirementsSatisfied())
        annotatorsSatisfiedRequirements
      }
    }

    new StanfordAnnotationPipeline(annotators.toList.asJava, pipeline)
  }

  private def transform[T](lang: String, st: StanfordAnnotator[T]): String => Seq[T] = {
    val output = Seq(st)
    val pipeline = annotationPipeline(output, lang)

    document: String => pipeline.annotate(document).head.asInstanceOf[Seq[T]]
  }

  def tokenize(lang: String): String => Seq[String] = transform(lang, Tokenize())

  def cleanxml(lang: String): String => Seq[String] = transform(lang, Cleanxml())

  def pos(lang: String): String => Seq[String] = transform(lang, Pos())

  def lemma(lang: String): String => Seq[String] = transform(lang, Lemma())

  def ner(lang: String): String => Seq[String] = transform(lang, Ner())

  def sentiment(lang: String): String => Seq[String] = transform(lang, Sentiment())

  def depparse(lang: String): String => Seq[String] = transform(lang, Depparse())

  def ssplit(lang: String): String => Seq[String] = {
    document: String => {
      val pipeline = languagePipeline(lang)
      val doc = new CoreDocument(document)
      pipeline.annotate(doc)
      doc.sentences().asScala.map(_.text())
    }
  }
}

object StanfordCoreNLPHelper extends SparkSessionWrapper {
  private val supportedLanguages: Array[String] =
    LanguagesConfiguration.stanfordNLPsupportedLanguages

  private val snlp = new StanfordCoreNLPHelper(supportedLanguages)
  /**
    * We need to share just 1 pipeline per executor because Stanford CoreNLP
    * models are very memory intensive.
    */
  private lazy val bSnlp: Broadcast[StanfordCoreNLPHelper] =
    currentSparkSession.sparkContext.broadcast(snlp)

  def apply(): Broadcast[StanfordCoreNLPHelper] = bSnlp

  def local(): StanfordCoreNLPHelper = snlp
}

class StanfordAnnotationPipeline(annotators: util.List[Annotator],
                                 pipeline: Seq[StanfordAnnotator[Any]]) extends
  AnnotationPipeline(annotators) {
  // Copied from
  // edu.stanford.nlp.pipeline.StanfordCoreNLP.annotate(edu.stanford.nlp.pipeline.CoreDocument)
  def annotate(document: String): Seq[Seq[Any]] = {
    val doc = new CoreDocument(document)
    // annotate the underlying Annotation
    super.annotate(doc.annotation())
    // wrap the sentences and entity mentions post annotation
    doc.wrapAnnotations()
    pipeline.map(_.transformation(doc))
  }
}

trait StanfordAnnotator[+T] {
  val outputColumn: String
  val annotator: String
  val transformation: CoreDocument => Seq[T]
  lazy val dependency: Seq[String] = Annotator.DEFAULT_REQUIREMENTS.getOrDefault(annotator, new
      util.LinkedHashSet[String]()).asScala.toSeq
  val schema: StructField = StructField(outputColumn, schemaFor[Seq[String]].dataType, nullable =
    true)
}

case class Tokenize(outputColumn: String = "")
  extends StanfordAnnotator[String] {

  val annotator: String = "tokenize"

  val transformation: CoreDocument => Seq[String] =
    doc => doc.tokens().asScala.map(_.word())
}

case class Cleanxml(outputColumn: String = "")
  extends StanfordAnnotator[String] {

  val annotator: String = "cleanxml"

  val transformation: CoreDocument => Seq[String] = doc => doc.tokens().asScala.map(_.word())
}

case class Lemma(outputColumn: String = "")
  extends StanfordAnnotator[String] {

  val annotator: String = "lemma"

  val transformation: CoreDocument => Seq[String] =
    doc => doc.sentences().asScala.flatMap(_.tokens().asScala).map(_.lemma())
}

case class Pos(outputColumn: String = "")
  extends StanfordAnnotator[String] {

  val annotator: String = "pos"

  val transformation: CoreDocument => Seq[String] =
    doc => doc.sentences().asScala.flatMap(_.posTags().asScala)
}

case class Ner(outputColumn: String = "")
  extends StanfordAnnotator[String] {
  val annotator: String = "ner"

  val transformation: CoreDocument => Seq[String] =
    doc => doc.sentences().asScala.flatMap(_.nerTags().asScala)
}

case class Sentiment(outputColumn: String = "")
  extends StanfordAnnotator[String] {
  val annotator: String = "sentiment"

  val transformation: CoreDocument => Seq[String] =
    doc => doc.sentences().asScala.map(_.sentiment())
}

// I don't know what you want to do with this. The problem here is that you need to do it in this
// depparse function because we can' t store SemanticGraph in dataframe row according to
// https://stackoverflow.com/questions/39096268/how-to-use-user-defined-types-in-spark-2-0
// and also https://issues.apache.org/jira/browse/SPARK-7768
/*
case class Depparse(outputColumn: String = "")
  extends StanfordAnnotator[SemanticGraph] {

  val annotator: String = "depparse"

  val transformation: CoreDocument => Seq[SemanticGraph] =
    doc => doc.sentences().asScala.map(_.dependencyParse())

  override val schema: StructField = StructField(outputColumn, DataTypes.BinaryType, nullable =
  true)
}
*/

case class Depparse(outputColumn: String = "")
  extends StanfordAnnotator[String] {

  val annotator: String = "depparse"

  val transformation: CoreDocument => Seq[String] =
    doc => doc.sentences().asScala.map(_.dependencyParse().toString(SemanticGraph.OutputFormat.XML))
}

/**
  * Used for have for each language same schema.
  * If for example we define "en" pipeline with Pos and Lemma and "de" pipeline with pos.
  * In "de" we add annotator Null with the same column as Lemma.
  * @param outputColumn Output column
  */
case class Null(outputColumn: String = "")
  extends StanfordAnnotator[Any] {

  val annotator: String = ""

  val transformation: CoreDocument => Seq[Any] =
    doc => null
}

case class StanfordPipelineWithLanguage(language: String, pipeline: Seq[StanfordAnnotator[Any]])


case class StanfordPipelines(columns: Seq[String], schema: Seq[StructField],
                             pipelines: Map[String, Seq[StanfordAnnotator[Any]]])

object StanfordPipelines {
  implicit def tupleToStanfordPipelineWithLanguage[T <: Product](productLike: T)
  : StanfordPipelineWithLanguage =
    StanfordPipelineWithLanguage(
      productLike.productElement(0).asInstanceOf[String],
      productLike.productIterator.drop(1).toList.asInstanceOf[Seq[StanfordAnnotator[Any]]]
    )

  def apply(pipelines: StanfordPipelineWithLanguage*): StanfordPipelines = {
    val columns = pipelines.flatMap(_.pipeline.map(_.outputColumn)).distinct
    val outputAnnotators = columns.map(Null)
    val map: Map[String, Seq[StanfordAnnotator[Any]]] = pipelines.map(pipelineWithLanguage =>
      pipelineWithLanguage.language -> outputAnnotators.map(none => {
        pipelineWithLanguage.pipeline.find(none.outputColumn == _.outputColumn) match {
          case Some(p) => p
          case None => none
        }
      })).toMap


    val values = map.values
    // validates if for each language we have same schema from annotator. We have same datatype in
    // output dataframe
    val schema = values.foldLeft(values.head) {
      (results, pipeline) => {
        results.zip(pipeline)
          .map {
            case (a, b: Null) => a
            case (a: Null, b) => b
            case (a, b) => if (a.schema != b.schema) {
              throw throw new IllegalArgumentException(
                s"Output column ${a.outputColumn} defines different schema for different language")
            } else a
          }
      }
    }

    StanfordPipelines(columns, schema.map(_.schema), map)
  }
}
