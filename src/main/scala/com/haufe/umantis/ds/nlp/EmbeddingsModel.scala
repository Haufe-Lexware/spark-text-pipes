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

import com.github.fommil.netlib.BLAS.{getInstance => blas}
import com.haufe.umantis.ds.nlp.params.{
  HasDefaultLanguage, HasLanguageCol, HasSupportedLanguages, HasTransformFunction}
import com.haufe.umantis.ds.embdict.EmbeddingsDictClient
import org.apache.spark.broadcast.{Broadcast => SparkBroadcast}
import org.apache.spark.ml.Model
import org.apache.spark.ml.linalg.{DenseVector, Vector}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.ScalaReflection.schemaFor
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.immutable.ListMap


/**
  * Params for [[EmbeddingsModel]].
  */
trait EmbeddingsModelBase extends Params
  with HasInputCol with HasOutputCol with HasMaxIter with HasStepSize with HasSeed
  with HasSupportedLanguages with HasDefaultLanguage with HasLanguageCol with HasTransformFunction {

  // TODO: This should be removed / refactored in EmbeddingsDictClient
  /**
    * The dimension of the code that you want to transform from words.
    * Default: 300
    * @group param
    */
  final val vectorSize = new IntParam(
    this,
    "vectorSize",
    "the dimension of codes after transforming from words (> 0)",
    ParamValidators.gt(0))
  setDefault(vectorSize -> 300)

  /** @group getParam */
  def getVectorSize: Int = $(vectorSize)

  setDefault(stepSize -> 0.025)
  setDefault(maxIter -> 1)
}

object EmbeddingsModel {
  def apply() = new EmbeddingsModel(EmbeddingsDictClient.broadcastedDict())
}
/**
  * Model that connects to a remote EmbeddingsDict server.
  */
class EmbeddingsModel(
    override val uid: String,
    bDict: SparkBroadcast[EmbeddingsDictClient]
)
  extends Model[EmbeddingsModel] with EmbeddingsModelBase with TransformerList {

  def this(bDict: SparkBroadcast[EmbeddingsDictClient]) =
    this(Identifiable.randomUID("EmbeddingsModel"),bDict)


  def analogy(language: String,
              positive: Array[String],
              negative: Array[String] = Array(),
              topN: Int = 10):
  DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    spark.createDataFrame(
      bDict.value.analogy(language, positive, negative, topN).toArray)
      .toDF("word", "similarity")
  }

  /**
    * Find "num" number of words closest in similarity to the given word, not
    * including the word itself.
    * @return a dataframe with columns "word" and "similarity" of the word and the cosine
    * similarities between the synonyms and the given word vector.
    */
  def findSynonyms(language: String, word: String, num: Int): DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    spark.createDataFrame(findSynonymsArray(language, word, num).toArray)
      .toDF("word", "similarity")
  }

  /**
    * Find "num" number of words whose vector representation is most similar to the supplied vector.
    * If the supplied vector is the vector representation of a word in the model's vocabulary,
    * that word will be in the results.
    * @return a dataframe with columns "word" and "similarity" of the word and the cosine
    * similarities between the synonyms and the given word vector.
    */
  def findSynonyms(language: String, vec: Vector, num: Int): DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    spark.createDataFrame(findSynonymsArray(language, vec, num).toArray)
      .toDF("word", "similarity")
  }

  /**
    * Find "num" number of words whose vector representation is most similar to the supplied vector.
    * If the supplied vector is the vector representation of a word in the model's vocabulary,
    * that word will be in the results.
    * @return a dataframe with columns "word" and "similarity" of the word and the cosine
    * similarities between the synonyms and the given word vector.
    */
  def findSynonyms(language: String, vec: Array[Float], num: Int): DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    spark.createDataFrame(findSynonymsArray(language, vec, num).toArray)
      .toDF("word", "similarity")
  }

  /**
    * Find "num" number of words whose vector representation is most similar to the supplied vector.
    * If the supplied vector is the vector representation of a word in the model's vocabulary,
    * that word will be in the results.
    * @return an array of the words and the cosine similarities between the synonyms given
    * word vector.
    */
  def findSynonymsArray(language: String, vec: Vector, num: Int): ListMap[String, Float] = {
    bDict.value.findSynonyms(language, vec, num)
  }

  /**
    * Find "num" number of words whose vector representation is most similar to the supplied vector.
    * If the supplied vector is the vector representation of a word in the model's vocabulary,
    * that word will be in the results.
    * @return an array of the words and the cosine similarities between the synonyms given
    * word vector.
    */
  def findSynonymsArray(language: String, vec: Array[Float], num: Int): ListMap[String, Float] = {
    bDict.value.findSynonyms(language, vec, num)
  }

  /**
    * Find "num" number of words closest in similarity to the given word, not
    * including the word itself.
    * @return an array of the words and the cosine similarities between the synonyms given
    * word vector.
    */
  def findSynonymsArray(language: String, word: String, num: Int): ListMap[String, Float] = {
    bDict.value.findSynonyms(language, word, num)
  }

  /** @group setParam */
  def setInputCol(value: String): this.type = {
    set(inputCol, value)
  }

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

//  val transformParallel: Dataset[_] => DataFrame = (dataset: Dataset[_]) => {
//    transformSchema(dataset.schema, logging = true)
//    val d = $(vectorSize)
//    val word2Vec = udf { sentence: Seq[String] =>
//      if (sentence.isEmpty) {
//        Vectors.sparse(d, Array.empty[Int], Array.empty[Double])
//      } else {
//        val sum = Array.fill[Float](d)(0)
//        val wordVectors = bDict.value.query(w2vlanguage, sentence.map(_.toLowerCase).toSet)
//        sentence.foreach { word =>
//          wordVectors.getOrElse(word, None) match {
//            case Some(v) => blas.saxpy(d, 1.0f, v, 1, sum, 1)
//            case None => ;
//          }
//        }
//        val norm = blas.snrm2(d, sum, 1)
//        if (norm > 0) {
//          blas.sscal(d, 1.0f / norm, sum, 1)
//        }
//        new DenseVector(sum.map(_.toDouble))
//      }
//    }
//    dataset.withColumn($(outputCol), word2Vec(col($(inputCol))))
//  }

  private val transformationFunction = {
    (language: String, sentence: Seq[String]) =>
      if (language == null || sentence == null) null else {
        val d = $(vectorSize)
        val embeddingsLanguage = if ($(supportedLanguages).contains(language))
          language
        else
          $(defaultLanguage)

        val sum = Array.fill[Float](d)(0)
        if (sentence.nonEmpty) {
          sentence.foreach { word =>
            bDict.value.query(embeddingsLanguage, word.toLowerCase) match {
              case Some(v) => blas.saxpy(d, 1.0f, v, 1, sum, 1)
              case None => ;
            }
          }
          val norm = blas.snrm2(d, sum, 1)
          if (norm > 0)
            blas.sscal(d, 1.0f / norm, sum, 1)
        }
        new DenseVector(sum.map(_.toDouble))
        // sum // to return Array[Float]
      }
  }

  def transform(data: Seq[Map[String, Any]]): Seq[Map[String, Any]] = {
    val res = data.map(p => {
      p + ($(outputCol) -> 
        transformationFunction(p($(languageCol)).asInstanceOf[String],
          p($(inputCol)).asInstanceOf[Seq[String]]))
    })
    res
  }
  
  val transformSerial: Dataset[_] => DataFrame = (dataset: Dataset[_]) => {
    transformSchema(dataset.schema, logging = true)
    val word2Vec = udf {transformationFunction}
    dataset.withColumn($(outputCol), word2Vec(col($(languageCol)), col($(inputCol))))
  }

  def getVector(language:String, word: String): Option[Array[Float]] = {
    bDict.value.query(language, word)
  }

  /**
    * Transform a sentence column to a vector column to represent the whole sentence. The transform
    * is performed by averaging all word vectors it contains.
    */
  setDefault(ParamPair(transformFunction, transformSerial: Dataset[_] => DataFrame))

//  def outputDataType: DataType = schemaFor[Array[Float]].dataType
  def outputDataType: DataType = schemaFor[DenseVector].dataType

  override def transformSchema(schema: StructType): StructType = {
    val languageType = schema($(languageCol)).dataType
    val inputType = schema($(inputCol)).dataType
    require(languageType == StringType,
      s"$this language column should be String but is $languageType")
    require(inputType == ArrayType(StringType),
      s"$this input column should be  ArrayType(StringType) but is $inputType")

    if (schema.fieldNames.contains($(outputCol))) {
      throw new IllegalArgumentException(s"Output column ${$(outputCol)} already exists.")
    }
    val outputFields = schema.fields :+
      StructField($(outputCol), outputDataType, nullable = false)
    StructType(outputFields)
  }

  override def copy(extra: ParamMap): EmbeddingsModel = {
    val copied = new EmbeddingsModel(uid, bDict)
    copyValues(copied, extra).setParent(parent)
  }
}
