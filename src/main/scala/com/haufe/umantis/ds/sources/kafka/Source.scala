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

/**
  * Defines the interface for all Kafka sources of sort.
  */
private[kafka]
trait Source {
  /**
    * Start the processing of (all) the topic(s).
    * @return Itself
    */
  def start(): this.type

  /**
    * Stops the processing of (all) the topic(s).
    * @return Itself
    */
  def stop(): this.type

  /**
    * Deletes the sink parquet file associated to (all) the topic(s).
    * @return Itself
    */
  def delete(): this.type

  /**
    * Stop, deletes, and start the processing of (all) the topic(s).
    * @return Itself
    */
  def reset(): this.type

//  /**
//    * Sets this source as "read only". This means that no kafka processing is done;
//    * but already processed data is available from persistent storage.
//    * @param value Enable "read only" or not.
//    * @return Itself
//    */
//  def readOnly(value: Boolean): this.type

  def status(): Map[String, String]
}
