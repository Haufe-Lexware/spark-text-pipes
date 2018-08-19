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

package com.haufe.umantis.ds.mongodb

import com.haufe.umantis.ds.spark.SparkIO
import org.apache.spark.sql.DataFrame
import com.mongodb.spark.config._
import com.mongodb.spark.sql._


class MongoInterface(val hostname: String,
                     val port: Int,
                     val username: Option[String] = None,
                     private val password: Option[String] = None)
  extends SparkIO {

  def loadCollection(database: String, collection: String): DataFrame = {
    val localReadConfig = ReadConfig(Map(
      "uri"-> {
        val noAuthCase = s"mongodb://$hostname:${port.toString}/$database"

        username match {
          case Some(uname) =>
            password match {
              case Some(pword) => s"mongodb://$uname:$pword@$hostname:${port.toString}/$database"
              case _ => noAuthCase
            }
          case _ => noAuthCase
        }
      },
      "collection" -> collection
    ))
    currentSparkSession.loadFromMongoDB(localReadConfig).toDF()
  }

  def saveCollection(dataFrame: DataFrame, database: String, collection: String): Unit = {
    val localWriteConfig = WriteConfig(Map(
      "uri"-> s"mongodb://$username:$password@$hostname:${port.toString}/$database",
      "collection" -> collection
    ))
    dataFrame.saveToMongoDB(localWriteConfig)
  }
}
