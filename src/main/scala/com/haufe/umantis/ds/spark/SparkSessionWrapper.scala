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
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.util.Try


trait SparkSessionWrapper {

  val sparkConf: Config = ConfigGetter.getConfig("/spark.conf")

  def localMaster: String = {
    println("#### CREATING LOCAL SPARK ####")
    "local[*]"
  }

  def clusterMaster: String = {
    // println("#### CREATING CLUSTER SPARK ####")
    sparkConf.getString("spark-configuration.master")
  }

  lazy val testing: Boolean = Try(
    sys.env.getOrElse("TESTING", "false").toBoolean
  ).getOrElse(false)

  lazy val debugging: Boolean = Try(
    sys.env.getOrElse("DEBUGGING", "false").toBoolean
  ).getOrElse(false)

  lazy val currentSparkSession: SparkSession = {
    val master: String = if (testing) localMaster else clusterMaster

    val conf = new SparkConf(true)
      .setMaster(master)
      .setAppName(sparkConf.getString("spark-configuration.app-name"))

    if (ConfigGetter.boolConf(sparkConf, "spark-configuration.use-elasticsearch"))
      conf.set("es.nodes", sparkConf.getString("spark-configuration.elasticsearch-host"))

    if (ConfigGetter.boolConf(sparkConf, "spark-configuration.use-cassandra")) {
      conf.set("spark.cassandra.connection.host",
        sparkConf.getString("spark-configuration.cassandra-host"))

      val username = sparkConf.getString("spark-configuration.cassandra-auth-username")
      if (! username.isEmpty)
        conf.set("spark.cassandra.auth.username", username)

      val password = sparkConf.getString("spark-configuration.cassandra-auth-password")
      if (! password.isEmpty)
        conf.set("spark.cassandra.auth.password", password)
    }

    SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
  }
}
