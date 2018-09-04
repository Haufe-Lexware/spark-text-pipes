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

package com.haufe.umantis.ds.utils

import com.twitter.util.SynchronizedLruMap
import org.slf4j.LoggerFactory

import scala.annotation.tailrec

import dispatch._
import Defaults._


/**
  * @param connectTimeout HTTP connection timeout, in ms
  * @param readTimeout    HTTP read timeout, in ms
  * @param cacheSize      Number of resolved URLs to maintain in cache
  */
class URLUnshortener(val connectTimeout: Int, val readTimeout: Int, val cacheSize: Int)
  extends Serializable {

  @transient lazy val cache = new SynchronizedLruMap[String, String](cacheSize)

  /**
    * Expand the given short URL
    *
    * @param address The URL
    * @return The unshortened URL
    */
  @tailrec
  final def expand(address: String): String = {

    cache.get(address) match {
      case Some(url) => url

      case _ =>
        val expandedURL: Option[String] =
          try {
            val future = Http.default(url(address))

            val response = future()
            Option(response.getHeaders.get("Location")) // returns null if Location not found

          } catch {
            case e: Exception =>
              //  URLUnshortener.LOGGER
              //    .warn("Problem while expanding {}", address: Any, e.getMessage: Any)
              None
          }

        expandedURL match {
          case None => address
          case Some(newUrl) =>
            cache.put(address.toString, newUrl)
            expand(newUrl)
        }
    }
  }

  /**
    * Returns true if the URL expands
    *
    * @param address The URL
    * @return If the URL expands
    */
  def doesExpand(address: String): Boolean = {
    address != expand(address)
  }
}


/**
  * Expand short urls. Works with all the major url shorteners
  * (t.co, bit.ly, fb.me, is.gd, goo.gl, etc).
  */
object URLUnshortener {
  private val LOGGER = LoggerFactory.getLogger(classOf[URLUnshortener])
  val DEFAULT_CONNECT_TIMEOUT = 1000
  val DEFAULT_READ_TIMEOUT = 1000
  val DEFAULT_CACHE_SIZE = 100000

  def apply(): URLUnshortener = {
    new URLUnshortener(
      URLUnshortener.DEFAULT_CONNECT_TIMEOUT,
      URLUnshortener.DEFAULT_READ_TIMEOUT,
      URLUnshortener.DEFAULT_CACHE_SIZE
    )
  }
}