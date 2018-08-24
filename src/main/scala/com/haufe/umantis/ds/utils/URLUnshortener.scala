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

import java.io.IOException
import java.net.{HttpURLConnection, Proxy, URL}

import com.google.common.io.Closeables
import com.twitter.util.SynchronizedLruMap
import org.slf4j.LoggerFactory

import scala.annotation.tailrec


/**
  * @param connectTimeout HTTP connection timeout, in ms
  * @param readTimeout    HTTP read timeout, in ms
  * @param cacheSize      Number of resolved URLs to maintain in cache
  */
class URLUnshortener(val connectTimeout: Int, val readTimeout: Int, val cacheSize: Int)
  extends Serializable {

  @transient lazy val cache = new SynchronizedLruMap[String, URL](cacheSize)

  /**
    * Expand the given short {@link URL}
    *
    * @param address The URL
    * @return The unshortened URL
    */
  @tailrec
  final def expand(address: URL): URL = {

    cache.get(address.toString) match {
      case Some(url) => url

      case _ =>
        var connection: HttpURLConnection = null
        //Connect & check for the location field
        val expandedURL: Option[URL] = try {
          connection = address.openConnection(Proxy.NO_PROXY).asInstanceOf[HttpURLConnection]
          connection.setConnectTimeout(connectTimeout)
          connection.setInstanceFollowRedirects(false)
          connection.setReadTimeout(readTimeout)
          connection.connect()
          val expandedURL: String = connection.getHeaderField("Location")
          if (expandedURL != null) {
            Some(new URL(expandedURL))
          } else {
            None
          }
        } catch {
          case e: Exception =>
            URLUnshortener.LOGGER
              .warn("Problem while expanding {}", address: Any, e.getMessage: Any)
            None
        } finally {
          try {
            if (connection != null)
              Closeables.close(connection.getInputStream, false)
          } catch {
            case e: IOException =>
              URLUnshortener.LOGGER.warn("Unable to close connection stream", e.getMessage: Any)
          }
        }

        expandedURL match {
          case None => address
          case Some(url) =>
            cache.put(address.toString, url)
            expand(url)
        }
    }
  }

  /**
    * Expand the given short URL in String format
    *
    * @param address The URL
    * @return The unshortened URL
    */
  def expand(address: String): String = {
    expand(new URL(address)).toString
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