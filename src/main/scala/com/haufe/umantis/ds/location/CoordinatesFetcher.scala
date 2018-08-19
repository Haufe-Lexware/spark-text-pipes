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

package com.haufe.umantis.ds.location


import com.haufe.umantis.ds.nlp.params._
import com.haufe.umantis.ds.spark.SparkIO
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.HasOutputCol
import org.apache.spark.sql.catalyst.ScalaReflection.schemaFor
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types.{StructField, StructType}
import com.twitter.util.SynchronizedLruMap


case class GeoLocationCacheItem(location: String, coordinates: GeoCoordinates)
case class CoordinatesFetcherCacheInput(
  df: DataFrame, locationCol: String, countryCodeCol: Option[String])
case class LocationAndCountry(location: String, countryCode: String)


class CoordinatesFetcherCache (
  private val defaultCache: Map[String, GeoCoordinates]
)
  extends Serializable
{
  @transient lazy val cache: SynchronizedLruMap[String, GeoCoordinates] = {
    val lruCacheSize = 1000000
    new SynchronizedLruMap[String, GeoCoordinates](lruCacheSize) ++= defaultCache
  }

  def getCoordinates(location: String, country: Option[String]): GeoCoordinates = {
    // spark does not support columns of None; it does support null though,
    // so here we convert null to None
    val nonNullCountryCode: Option[String] = country match {
      case Some(cc) => Option(cc)
      case _ => None
    }

    val locationAndCountry = s"$location$nonNullCountryCode"
    if (cache.contains(locationAndCountry))
      cache(locationAndCountry)
    else {
      val coordinates = Text2GeolocationClient.getCoordinates(location, nonNullCountryCode)
      cache(locationAndCountry) = coordinates
      coordinates
    }
  }
}

object CoordinatesFetcherCache extends SparkIO {
  val defaultCache: Map[String, GeoCoordinates] = {
    import currentSparkSession.implicits._

    try {
      readDataFrame("locationCache")
        .as[GeoLocationCacheItem]
        .collect()
        .map(item => (item.location, item.coordinates))
        .toMap
    } catch {
      case _: Any => Map[String, GeoCoordinates]()
    }
  }

  def fillCache(input: Seq[CoordinatesFetcherCacheInput], clearCache: Boolean = true): Unit = {
    import currentSparkSession.implicits._

    val myCache = CoordinatesFetcherCache()

    if (clearCache)
      myCache.cache.clear()

    // filling the cache by putting all DataFrames together with the same format
    // collecting them as LocationAndCountry Array, and in parallel, for each location and country
    // calling getCoordinates of the local cache object (in the driver) so to fill it
    input.map(item => {
      val cols = (item.countryCodeCol match {
        case Some(c) => Seq(item.locationCol, c)
        case _ => Seq(item.locationCol)
      })
        .map(col)

      val countryCodeString = item.countryCodeCol match {case Some(c) => c; case _ => ""}

      val locationData = item.df
        .select(cols:_*)
        .distinct()
        .withColumnRenamed(item.locationCol, "location")
        // no-op if source col does not exist
        .withColumnRenamed(countryCodeString, "countryCode")

      item.countryCodeCol match {
        case Some(_) => locationData
        case None => locationData
          .withColumn("countryCode", lit(null)) // lit(None) is not supported
      }
    })
      .reduce(_ union _)
      .as[LocationAndCountry]
      .collect()
      .par
      .foreach(item => {
        println(s"Getting coordinates for ${item.location}")
        myCache.getCoordinates(item.location, Some(item.countryCode))
      })

    val cacheDf = myCache.cache.toMap.toSeq.toDF("location", "coordinates")
    writeDataFrame(cacheDf, "locationCache")
    println(s"CoordinatesFetcherCache size: ${myCache.cache.size}")
  }

  lazy val geoFetcherCache: CoordinatesFetcherCache = new CoordinatesFetcherCache(defaultCache)

  def apply(): CoordinatesFetcherCache = geoFetcherCache
}


class CoordinatesFetcher(override val uid: String)
  extends Transformer
    with HasLocationCol with HasCountryCol with HasOutputCol with Logging {

  def this() = this(Identifiable.randomUID("LocationFetcher"))

  def setOutputCol(value: String): this.type = set(outputCol, value)
  setDefault(countryCol, "")

  val coordinatesFetcherCache: CoordinatesFetcherCache = CoordinatesFetcherCache()
  val coordinatesFetcherCacheB: Broadcast[CoordinatesFetcherCache] =
    SparkSession.builder().getOrCreate().sparkContext.broadcast(coordinatesFetcherCache)

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)

    val getCoordinatesWithCountry = udf{ (location: String, country: String) =>
      coordinatesFetcherCacheB.value.getCoordinates(location, Some(country))
    }

    val getCoordinates = udf{ (location: String) =>
      coordinatesFetcherCacheB.value.getCoordinates(location, None)
    }

    if (dataset.columns.contains($(countryCol)))
      dataset.withColumn($(outputCol),
        getCoordinatesWithCountry(col($(locationCol)), col($(countryCol))))
    else
      dataset.withColumn($(outputCol),
        getCoordinates(col($(locationCol))))
  }

  override def transformSchema(schema: StructType): StructType = {
    val locationType = schema($(locationCol)).dataType
    require(locationType == schemaFor[String].dataType)

    if (schema.contains($(countryCol))) {
      val countryType = schema($(countryCol)).dataType
      require(countryType == schemaFor[String].dataType)
    }

    if (schema.fieldNames.contains($(outputCol))) {
      throw new IllegalArgumentException(s"Output column ${$(outputCol)} already exists.")
    }
    val outputFields = schema.fields :+
      StructField($(outputCol), schemaFor[GeoCoordinates].dataType, nullable = false)
    StructType(outputFields)
  }

  override def copy(extra: ParamMap): CoordinatesFetcher = {
    new CoordinatesFetcher()
      .setLocationCol(getLocationCol)
      .setCountryCol(getCountryCol)
  }
}