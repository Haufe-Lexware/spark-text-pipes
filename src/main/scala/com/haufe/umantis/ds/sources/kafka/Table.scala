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

package com.haufe.umantis.ds.sources.kafka

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction

/**
  * Defines a helper class to perform specific transformations to DataFrames obtained by Kafka.
  *
  * @param name The final part of the name of the topic
  * @param transformation The transformation to apply
  * @param version The version of the topic
  * @param refreshTime The refresh time (in seconds) after which the parquet file is re-read.
  */
case class Table(
  name: String,
  transformation: DataFrame => DataFrame = {df => df},
  version: String = "v1",
  refreshTime: Option[Int] = None /* seconds */,

  // for avro keys
  uniqueEntityKey: Option[UserDefinedFunction] =
    Some(GenericUniqueIdentityKeys.EntityAndTimestampFromAvroKeyUDF)

  // for non-avro key (String)
  // uniqueEntityKey: UserDefinedFunction = UniqueIdentityKeys.EntityAndTimestampFromStringKeyUDF
)
