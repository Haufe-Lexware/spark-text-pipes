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
import com.haufe.umantis.ds.utils.BackendType.BackendType


object BackendType extends Enumeration {
  type BackendType = Value
  val Sync, Async = Value
}


case class CheckedURL(
                       origUrl: String,
                       finalUrl: String,
                       connects: Boolean,
                       numRedirects: Int
                     )

abstract class HttpBackend extends Serializable {

  def doExpandURL(address: CheckedURL): Option[String]

//  import scala.concurrent._
//  import scala.concurrent.duration._
//
//  def runWithTimeout[T](timeoutMs: Long)(f: => T) : Option[T] = {
//    Await.result(Future(f), timeoutMs milliseconds).asInstanceOf[Option[T]]
//  }
//
//  def runWithTimeout[T](timeoutMs: Long, default: T)(f: => T) : T = {
//    runWithTimeout(timeoutMs)(f).getOrElse(default)
//  }

//  def expandURL(address: CheckedURL): Option[String] = doExpandURL(address)

//  @tailrec
//  final def expandURL(address: CheckedURL, nrTry: Int = 1): Option[String] = {
//    val res = runWithTimeout(5000) {doExpandURL(address)}
//    res match {
//      case Some(optStr) => optStr
//      case None =>
//        if (nrTry > 5) {
//          println(s"timeout in http backend: ${address.origUrl}, $nrTry")
//          None
//        }
//        else
//          expandURL(address, nrTry + 1)
//    }
//  }
}

/**
  * @param connectTimeout HTTP connection timeout, in ms
  * @param readTimeout    HTTP read timeout, in ms
  */
class AsyncHttpBackend(val connectTimeout: Int, val readTimeout: Int) extends HttpBackend {

  def doExpandURL(address: CheckedURL): Option[String] = {

    val http = dispatch.Http.withConfiguration(_
      .setConnectTimeout(connectTimeout)
      .setReadTimeout(readTimeout)
    )

    val future = http(url(address.finalUrl))
    val response = future()

    // response.getHeaders.get(.) returns null if "Location" not found
    Option(response.getHeaders.get("Location"))
  }

}

/**
  * @param connectTimeout HTTP connection timeout, in ms
  * @param readTimeout    HTTP read timeout, in ms
  */
class SyncHttpBackend(val connectTimeout: Int, val readTimeout: Int) extends HttpBackend {

  def doExpandURL(address: CheckedURL): Option[String] = {
    val http = scalaj.http.Http
    val response = http(address.finalUrl)
      .timeout(connTimeoutMs = connectTimeout, readTimeoutMs = readTimeout)
      .asString
    response.location
  }
}

/**
  * @param backend:       The backend to use
  * @param connectTimeout HTTP connection timeout, in ms
  * @param readTimeout    HTTP read timeout, in ms
  * @param cacheSize      Number of resolved URLs to maintain in cache
  */
class URLUnshortener(
                      val backend: BackendType,
                      val connectTimeout: Int,
                      val readTimeout: Int,
                      val cacheSize: Int
                    )
  extends Serializable {

  @transient lazy val cache = new SynchronizedLruMap[String, CheckedURL](cacheSize)

  /**
    * Expand the given short URL
    *
    * @param address The URL
    * @param httpBackend    The HttpBackend class to use
    * @return A CheckedURL case class with info about finalURL, connection, and number of redirects
    */
  @tailrec
  final def doExpand[T <: HttpBackend](address: CheckedURL, httpBackend: T): CheckedURL = {

    cache.get(address.origUrl) match {
      case Some(url) => url

      case _ =>
        val expandedURL: Either[CheckedURL, Option[String]] =
          try {
            Right(httpBackend.doExpandURL(address))
          } catch {
            case e: Exception =>
              URLUnshortener.LOGGER
                .warn("Problem while expanding {}", address: Any, e.getMessage: Any)
              Left(
                CheckedURL(address.origUrl, address.finalUrl, connects=false, address.numRedirects)
              )
          }

        expandedURL match {
          case Left(value) => value

          case Right(value) => value match {
            case None =>
              val checkedURL =
                CheckedURL(address.origUrl, address.finalUrl, connects = true, address.numRedirects)
              cache.put(address.origUrl, checkedURL)
              checkedURL

            case Some(newUrl) =>
              if (address.finalUrl == newUrl || address.numRedirects > 10) {
                // we avoid loops (e.g. infinite redirects because we're not holding a session)
                CheckedURL(address.origUrl, newUrl, connects = true, address.numRedirects)
              } else {
                doExpand(
                  CheckedURL(address.origUrl, newUrl, connects = true, address.numRedirects + 1),
                  httpBackend
                )
              }
          }
        }
    }
  }

  def expand(address: String): CheckedURL = {
    val httpBackend = backend match {
      case BackendType.Sync => new SyncHttpBackend(connectTimeout, readTimeout)
      case BackendType.Async => new AsyncHttpBackend(connectTimeout, readTimeout)
    }

    doExpand(CheckedURL(address, address, connects=false, numRedirects = 0), httpBackend)
  }


  /**
    * Returns true if the URL expands
    *
    * @param address The URL
    * @return If the URL expands
    */
  def doesExpand(address: String): Boolean = {
    val checkedURL = expand(address)
    checkedURL.numRedirects > 0
  }
}


/**
  * Expand short urls. Works with all the major url shorteners
  * (t.co, bit.ly, fb.me, is.gd, goo.gl, etc).
  */
object URLUnshortener {
  private val LOGGER = LoggerFactory.getLogger(classOf[URLUnshortener])
  val DEFAULT_CONNECT_TIMEOUT = 3000
  val DEFAULT_READ_TIMEOUT = 3000
  val DEFAULT_CACHE_SIZE = 100000
  val DEFAULT_BACKEND: BackendType = BackendType.Sync

  def apply(): URLUnshortener = {
    new URLUnshortener(
      URLUnshortener.DEFAULT_BACKEND,
      URLUnshortener.DEFAULT_CONNECT_TIMEOUT,
      URLUnshortener.DEFAULT_READ_TIMEOUT,
      URLUnshortener.DEFAULT_CACHE_SIZE
    )
  }
}