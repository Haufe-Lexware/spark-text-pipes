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

package com.haufe.umantis.ds.spark

import com.haufe.umantis.ds.utils.ConfigGetter
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

import scala.util.Try


trait SparkSessionWrapper {

  lazy val currentSparkSession: SparkSession = {
    sys.env.get("TESTING") match {
      case Some(value) =>
        if (Try(value.toBoolean).getOrElse(false)) sessionLocal else sessionCluster

      case _ => sessionCluster
    }
  }

  lazy val sessionLocal: SparkSession = {
    println("#### CREATING LOCAL SPARK ####")

    SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()
  }

  lazy val sessionCluster: SparkSession = {
//    println("#### CREATING CLUSTER SPARK ####")

    val config: Config = ConfigGetter.getConfig("/spark.conf")

    SparkSession
      .builder()
      .master(config.getString("spark-configuration.master"))
//      .config("es.nodes", config.getString("spark-configuration.elasticsearch-host"))
      .appName(config.getString("spark-configuration.app-name"))
      .getOrCreate()
  }
}
