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

package com.haufe.umantis.ds.embdict

import akka.actor.{ActorSelection, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout
import com.haufe.umantis.ds.embdict.messages._
import com.haufe.umantis.ds.nlp.ColnamesText
import com.haufe.umantis.ds.spark.IO
import com.haufe.umantis.ds.utils.{ConfigGetter, _}
import com.twitter.util.SynchronizedLruMap
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.apache.spark.broadcast.{Broadcast => SparkBroadcast}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col

import scala.collection.immutable.ListMap
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try

import com.haufe.umantis.ds.nlp.LanguagesConfiguration

class EmbeddingsDictClient(

                            private val defaultCache: Map[String, Map[String,
                              Option[Array[Float]]]],
                            val supportedLanguages: Array[String],
                            val serverConfigurationString: String,
                            val clientConfigurationString: String

                          ) extends Serializable {

  val languages: Array[String] = supportedLanguages

  @transient lazy val clientConfig: Config =
    ConfigFactory.parseString(clientConfigurationString)

  @transient lazy val serverConfig: Config =
    ConfigFactory.parseString(serverConfigurationString)

  @transient lazy val cache: Map[String, SynchronizedLruMap[String, Option[Array[Float]]]] = {
    val lruCacheSize = clientConfig.getInt("embdict-client.lru-cache-size")

    languages.map(lang => lang -> {
      val lrumap = new SynchronizedLruMap[String, Option[Array[Float]]](lruCacheSize)
      defaultCache(lang).foreach { case (k, v) => lrumap.put(k, v) }
      //      lrumap ++= defaultCache(lang)
      //      new LruMap[String, Option[Array[Float]]](lruCacheSize)
      lrumap
    }).toMap
  }

  @transient lazy val remoteDict: Map[String, ActorSelection] = {
    import scala.util.Random
    val name = Random.alphanumeric.take(10).mkString("")
    val as = ActorSystem(name, serverConfig.resolve())

    val serializer = clientConfig.getString("embdict-client.serializer")
    val serverHost = clientConfig.getString("embdict-client.server-host")
    val serverPortKryo = clientConfig.getString("embdict-client.server-port-kryo")
    val serverPortJava = clientConfig.getString("embdict-client.server-port-java")
    val serverPort = serializer match {
      case "java" => serverPortJava
      case "kryo" => serverPortKryo
    }
    val remoteActorBase = s"akka.tcp://EmbeddingsDict@$serverHost:$serverPort/user/"

    // parallel dictionary implementations
    val parallelDicts = languages.map(lang =>
      lang -> as.actorSelection(s"$remoteActorBase${lang}_dict")
    ).toMap

    // serial dictionary implementations
    val serialDicts = languages.map(lang =>
      s"${lang}_serial" -> as.actorSelection(s"$remoteActorBase${lang}_dict_serial")
    ).toMap

    parallelDicts ++ serialDicts
  }

  def getRemoteDict(language: String): ActorSelection = {
    remoteDict.get(language) match {
      case Some(as) => as
      case _ => throw new DictionaryLanguageNotFoundException(
        s"Language $language was not found in dictionary.")
    }
  }

  def status(): Map[String /* language */, String /* status */] = {
    remoteDict.keys.map(lang => {
      s"embdict.$lang" ->
        Try(queryRemote(lang, "0") match { case _ => "working" }).getOrElse("fail")
    })
      .toMap
  }

  def fillCache(df: DataFrame,
                textColumn: ColnamesText): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    // collecting all words for each language
    languages.foreach(lang => {
      val allWords =
        df.filter(col(textColumn.language) === lang).flatMap(
          r => r.getAs[Seq[String]](textColumn.cleanWords).map(_.toLowerCase)
        )
          .collect
          .toSet
      //      println(s"$lang size: ${allWords.size}")

      // filling the cache by querying all words
      allWords.foreach(w => query(lang, w))
    })
  }

  def fillCacheMultiple(input: Map[DataFrame, Array[ColnamesText]]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    // collecting all words for each language
    languages.foreach(lang => {
      val allWords = input.flatMap(in => {
        val df = in._1
        val textColumns = in._2

        textColumns.flatMap(textCol => {
          // all words for each columnName in input
          df.filter(col(textCol.language) === lang).flatMap(
            r => r.getAs[Seq[String]](textCol.cleanWords).map(_.toLowerCase)
          )
            .collect
            .toSet
        })
          .toSet
      })
        .toSet

      //      println(s"$lang size: ${allWords.size}")

      // filling the cache by querying all words
      //      query(lang, allWords)
      allWords.foreach(w => query(lang, w))
    })
  }

  def clearCache(): Unit = {
    languages.foreach(lang => {
      cache(lang).clear()
    })
  }

  def getCacheSize: Map[String, Int] = {
    cache.map(c => (c._1, c._2.size))
  }

  def saveCache(cacheName: String = "cache"): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    cache.foreach(c => {
      val lang = c._1
      val filename = s"ft/$cacheName.$lang"
      val lrumap = c._2.toMap.toSeq.toDF("word", "vector")

      IO.writeDataFrame(lrumap, filename)
    })
  }

  @transient implicit lazy val timeout: Timeout = Timeout(2 seconds)

  // should be made async.
  def query(language: String, word: String, serialDictImpl: Boolean = false)
  : Option[Array[Float]] = {
    cache(language).getOrElse(word, queryRemote(language, word, serialDictImpl))
  }

  @inline
  def queryRemote(language: String, word: String, serialDictImpl: Boolean = false)
  : Option[Array[Float]] = {
    val actorName = if (serialDictImpl) s"${language}_serial" else language
    val future = getRemoteDict(actorName) ? WordVectorQuery(word)
    val vec = Await.result(future, timeout.duration).asInstanceOf[VectorResponse].vector
    cache(language.manualIntern())(word.manualIntern()) = vec
    vec
  }

  // TODO
  //  def queryWithIndex(language: String, word: String, serialDictImpl: Boolean = false)
  //  : VectorResponseWithIndex = _

  def queryRemoteWithIndex(language: String, word: String, serialDictImpl: Boolean = false)
  : VectorResponseWithIndex = {
    val actorName = if (serialDictImpl) s"${language}_serial" else language
    val future = getRemoteDict(actorName) ? WordVectorQueryWithIndex(word)
    val idxAndVec = Await.result(future, timeout.duration).asInstanceOf[VectorResponseWithIndex]

    // TODO
    //    cache(language)(word) = idxAndVec
    idxAndVec
  }

  // disabled, not thread safe
  //  def query(language: String, queryWords: Set[String]): Map[String, Option[Array[Float]]] = {
  //    val wordVectorsWeKnow = cache(language).filterKeys(queryWords)
  //    val wordsWeIgnore = queryWords -- wordVectorsWeKnow.keys
  //
  //    if (wordsWeIgnore.isEmpty) {
  //      wordVectorsWeKnow.toMap
  //    } else {
  //      val longTimeout = 5 seconds
  //      val newWordVectors = wordsWeIgnore.grouped(10).flatMap(groupOfWordsToLookUp => {
  //        val future = dict(language).ask(WordsVectorsQuery(groupOfWordsToLookUp))(longTimeout)
  //        Await.result(future, longTimeout).asInstanceOf[VectorsMapResponse].vectors
  //      })
  //        .toMap
  //
  //      cache(language) ++= newWordVectors
  //      newWordVectors ++ wordVectorsWeKnow
  //    }
  //  }

  def findSynonyms(language: String,
                   wordOrVec: AnyRef, /* Vector, Array[Float], or String */
                   num: Int,
                   serialDictImpl: Boolean = false)
  : ListMap[String, Float] = {
    val actorName = if (serialDictImpl) s"${language}_serial" else language
    val request = wordOrVec match {
      case vector: Vector => SynonymQueryVector(vector, num)
      case array: Array[Float] => SynonymQueryArray(array, num)
      case word: String => SynonymQueryWord(word, num)
    }
    val future = getRemoteDict(actorName) ? request
    processFindSynonymsResult(future)
  }

  def processFindSynonymsResult(future: Future[Any]): ListMap[String, Float] = {
    Await.result(future, timeout.duration).asInstanceOf[SynonymResponse].synonyms match {
      case Some(v) => v
      case None => ListMap(("", 0))
    }
  }

  def analogy(language: String, positive: Array[String], negative: Array[String], topN: Int = 10):
  ListMap[String, Float] = {
    val query = AnalogyQuery(positive = positive, negative = negative, size = topN)
    val future = getRemoteDict(language) ? query
    processFindSynonymsResult(future)
  }
}

object EmbeddingsDictClient extends ConfigGetter {

  private case class Data(word: String, vector: Option[Array[Float]])

  val clientConfiguration: Config = getConfig("/embeddings_dict_client.conf")

  val serializer: String = {
    val testing = Try(clientConfiguration.getString("embdict-client.testing").toBoolean)
      .getOrElse(false)
    val serializer = clientConfiguration.getString("embdict-client.serializer")
    if (testing || serializer == "java") "java" else "kryo"
  }
  println(s"### EmbeddingsDictClient using $serializer serializer")

  val serverConfigurationString: String = {
    val serverConf = getConfig("/embeddings_dict_akka_client.conf")
    val conf = if (serializer == "kryo")
      serverConf.withFallback(getConfig("/kryo_serializer.conf"))
    else
      serverConf

    conf.makeConcise
  }

  val clientConfigurationString: String = clientConfiguration
    .withValue("embdict-client.serializer", ConfigValueFactory.fromAnyRef(serializer))
    .makeConcise

  val languages: Array[String] = LanguagesConfiguration.supportedLanguages

  val defaultCache: Map[String, Map[String, Option[Array[Float]]]] = getCache("cache")

  def getCache(cacheName: String): Map[String, Map[String, Option[Array[Float]]]] = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    languages.map(lang => lang -> {
      val filename = s"ft/$cacheName.$lang"
      try {
        IO.getDF(filename)
          .as[Data]
          .collect()
          .map(wordVector => (wordVector.word.manualIntern(), wordVector.vector))
          .toMap
      } catch {
        case _: Any => Map[String, Option[Array[Float]]]()
      }
    }).toMap.map(identity)
  }

  private lazy val dict: EmbeddingsDictClient = new EmbeddingsDictClient(
    defaultCache,
    languages,
    serverConfigurationString,
    clientConfigurationString
  )
  private lazy val bDict: SparkBroadcast[EmbeddingsDictClient] =
    SparkSession.builder().getOrCreate().sparkContext.broadcast(dict)

  def apply(cacheName: String = "cache"): EmbeddingsDictClient = {
    if (cacheName != "cache")
      new EmbeddingsDictClient(
        getCache(cacheName),
        languages,
        serverConfigurationString,
        clientConfigurationString
      )
    else
      dict
  }

  def broadcastedDict(cacheName: String = "cache"): SparkBroadcast[EmbeddingsDictClient] = {
    if (cacheName != "cache")
      SparkSession.builder().getOrCreate()
        .sparkContext.broadcast(apply(cacheName))
    else
      bDict
  }

  def fillEmbeddingsCache(input: Map[DataFrame, Array[ColnamesText]],
                          clearCache: Boolean = true,
                          embDict: EmbeddingsDictClient = dict /* the default one */ ,
                          providedCacheName: String = "tmpcache"): Unit = {
    val cacheName = if (embDict == dict) "cache" else providedCacheName

    if (clearCache) {
      embDict.clearCache()
      embDict.saveCache(cacheName)
    }
    embDict.fillCacheMultiple(input)
    embDict.saveCache(cacheName)
    embDict.getCacheSize.foreach(x => println(s"EmbeddingsDict cache size: ${x._1} ${x._2}"))
  }
}



/**
  * Exception thrown if a tenant does not exist.
  * @param message The message of the exception.
  */
class DictionaryLanguageNotFoundException(message: String) extends Exception(message) {

  def this(message: String, cause: Throwable) {
    this(message)
    initCause(cause)
  }

  def this(cause: Throwable) {
    this(Option(cause).map(_.toString).orNull, cause)
  }

  def this() {
    this(null: String)
  }
}
