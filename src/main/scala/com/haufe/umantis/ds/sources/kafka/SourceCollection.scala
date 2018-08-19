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

import scala.collection.parallel.immutable.ParMap


/**
  * A trait to define the start/stop/delete/reset behaviour for the classes handling
  * the processing of multiple Kafka topics.
  *
  * @tparam T The type of the class contained in the sources Map.
  */
private[kafka]
trait SourceCollection[T <: Source] {
  /**
    * Parallel Map containing the lower-level classes processing the Kafka topics.
    */
  val sources: ParMap[String, T]

  /**
    * Start the processing of (all) the topic(s).
    * @return Itself
    */
  def start(): this.type = {
    sources.foreach{ case (_, source) => source.start() }
    this
  }

  /**
    * Stops the processing of (all) the topic(s).
    * @return Itself
    */
  def stop(): this.type = {
    sources.foreach{ case (_, source) => source.stop() }
    this
  }

  /**
    * Deletes the sink parquet file associated to (all) the topic(s).
    * @return Itself
    */
  def delete(): this.type = {
    sources.foreach{ case (_, source) => source.delete() }
    this
  }

  /**
    * Stop, deletes, and start the processing of (all) the topic(s).
    * @return Itself
    */
  def reset(): this.type = {
    sources.foreach{ case (_, source) => source.reset() }
    this
  }

//  /**
//    * Sets this source as "read only". This means that no kafka processing is done;
//    * but already processed data is available from persistent storage.
//    * @param value Enable "read only" or not.
//    * @return Itself
//    */
//  def readOnly(value: Boolean): this.type = {
//    sources.foreach{ case (_, source) => source.readOnly(value) }
//    this
//  }

  /**
    * Returns status information
    * @return A Map of the status key and its value
    */
  def status(): Map[String, String] = {
    sources.foldLeft(Map[String, String]()){
      case (currentStatus, (name, source)) =>
        currentStatus ++ source.status().map{ case (key, value) => s"$name.$key" -> value}
    }
  }
}
