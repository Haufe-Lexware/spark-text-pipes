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

package com.haufe.umantis.ds.wmd

import com.haufe.umantis.ds.embdict.EmbeddingsDictClient
import com.haufe.umantis.ds.utils._
import com.twitter.util.SynchronizedLruMap
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.broadcast.Broadcast

sealed trait DistanceType {
  private[wmd] val distanceType: Int

  def compute(v1: Seq[Float], v2: Seq[Float]): Float

  def dotProduct(v1: Seq[Float], v2: Seq[Float]): Float = {
    var i = 0
    var dotProduct = 0f
    val size = v1.length
    while (i < size) {
      dotProduct += v1(i) * v2(i)
      i += 1
    }
    dotProduct
  }
}

case object EuclideanDistance extends DistanceType {
  override private[wmd] val distanceType: Int = 0

  override def compute(v1: Seq[Float], v2: Seq[Float]): Float =
    math.sqrt(dotProduct(v1, v1) + dotProduct(v2, v2) - 2 * dotProduct(v1, v2)).toFloat
}

case object EuclideanDistanceNormalized extends DistanceType {
  override private[wmd] val distanceType: Int = 4

  override def compute(v1: Seq[Float], v2: Seq[Float]): Float =
    math.sqrt(2 - 2 * dotProduct(v1, v2)).toFloat
}

case object CosineDistanceNormalized extends DistanceType {
  override private[wmd] val distanceType: Int = 1

  override def compute(v1: Seq[Float], v2: Seq[Float]): Float =
    1 - dotProduct(v1, v2)
}

case object CosineDistance extends DistanceType {
  override private[wmd] val distanceType: Int = 2

  override def compute(v1: Seq[Float], v2: Seq[Float]): Float =
    1 - (dotProduct(v1, v2) / math.sqrt(dotProduct(v1, v1)) / math.sqrt(dotProduct(v2, v2))).toFloat
}

case object TanimotoDistance extends DistanceType {
  override private[wmd] val distanceType: Int = 3

  override def compute(v1: Seq[Float], v2: Seq[Float]): Float = {
    val dotProdV1V2 = dotProduct(v1, v2)
    1 - dotProdV1V2 / (dotProduct(v1, v1) + dotProduct(v2, v2) - dotProdV1V2)
  }
}

case object TanimotoDistanceNormalized extends DistanceType {
  override private[wmd] val distanceType: Int = 5

  override def compute(v1: Seq[Float], v2: Seq[Float]): Float = {
    val dotProdV1V2 = dotProduct(v1, v2)
    1 - dotProdV1V2 / (2 - dotProdV1V2)
  }
}

class WordMoverDistanceCalculator(val bdict: Broadcast[EmbeddingsDictClient],
                                  val supportedLanguages: Array[String],
                                  val defaultLanguage: String,
                                  val clientConfigurationString: String)
extends Serializable {

  @transient private lazy val clientConfig: Config =
    ConfigFactory.parseString(clientConfigurationString)

  @transient private lazy val cache: SynchronizedLruMap[(String, String, Int), Option[Float]] = {
    val lruCacheSize = clientConfig.getInt("embdict-client.lru-cache-size")
    val maxSize = supportedLanguages.length * supportedLanguages.length * lruCacheSize

    new SynchronizedLruMap[(String, String, Int), Option[Float]](maxSize)
  }

  @transient lazy val languageMap: Map[String, Int] = {
    supportedLanguages.zipWithIndex.toMap
  }


  @transient private lazy val defaultIndex = languageMap.getOrElse(defaultLanguage, 0)


  def computeDistance(document1: Seq[(String, Int)],
                      document2: Seq[(String, Int)],
                      language1: String = defaultLanguage,
                      language2: String = defaultLanguage,
                      distanceType: DistanceType = EuclideanDistance): Float = {
    def wordCount(doc: Seq[(String, Int)]): Float =
      doc.foldLeft(0.0F) { (sum, i) => sum + i._2 }

    Seq(
      (document1, document2, language1, language2, distanceType, wordCount(document1)),
      (document2, document1, language2, language1, distanceType, wordCount(document2))
    )
      .flatMap {
      // one way Word Mover Distance function
      case (docA, docB, langA, langB, distType, docAWordCount) =>
        docA.flatMap { case (wordA, wordAFrequency) =>
          docB.flatMap { case (wordB, _) =>
            val distance = cache.getOrElse(
              createKey(wordA, langA, wordB, langB, distType),
              computeWordDistanceAndUpdateCache(wordA, langA, wordB, langB, distType)
            )
            // println(s"$wordA $wordB $distance")
            distance
          }
            // minimum (over docB) of distance(wordAi, wordBj)
            .reduceOption(_ min _)
            .map(minDistance => minDistance * wordAFrequency / docAWordCount)
        }
          // sum (over docA) of min(distance(wordAi, wordBj)
          .reduceOption(_ + _)
    }
      // returns the highest of distance(docA, docB), distance(docB, docA)
      .reduceOption(_ max _)
      .getOrElse(Float.MaxValue)
  }

  private def computeWordDistanceAndUpdateCache(word1: String, language1: String,
                                                word2: String, language2: String,
                                                distanceType: DistanceType)
  : Option[Float] = {

    val v1o = bdict.value.query(language1, word1)
    val v2o = bdict.value.query(language2, word2)
    val distance = (v1o, v2o) match {
      case (Some(v1), Some(v2)) => Some(distanceType.compute(v1, v2))
      case _ => None
    }
    cache.put(
      createKey(word1.manualIntern(), language1, word2.manualIntern(), language2, distanceType),
      distance
    )
    distance
  }


  /**
    * Create unique Ints based on language map and computation type. It iss a part of the key in
    * cache so we can determine what combination we have in the cache.
    * We use bitwise operations to calculate it.
    * Maximum number of languages is [[2^8]], same as computations types
    */
  private def computeMetadata(language1: String, language2: String, computation: DistanceType)
  : Int = {
    languageMap.getOrElse(language1, defaultIndex) << 16 |
    languageMap.getOrElse(language2, defaultIndex) << 8  |
    computation.distanceType
  }

  private def createKey(word1: String, language1: String,
                        word2: String, language2: String,
                        distanceType: DistanceType)
  : (String, String, Int) = {
    if (word1 < word2)
      (word1, word2, computeMetadata(language1, language2, distanceType))
    else
      (word2, word1, computeMetadata(language2, language1, distanceType))
  }
}

object WordMoverDistanceCalculator {
  def apply(): WordMoverDistanceCalculator = new WordMoverDistanceCalculator(
    EmbeddingsDictClient.broadcastedDict(),
    EmbeddingsDictClient.languages,
    "en",
    EmbeddingsDictClient.clientConfigurationString
  )
}