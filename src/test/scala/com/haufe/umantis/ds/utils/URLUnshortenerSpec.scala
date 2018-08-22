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

import org.scalatest._
import Matchers._
import com.haufe.umantis.ds.tests.BaseSpec


class URLUnshortenerSpec extends BaseSpec {

  // created to expand to google
  val expandableURL = "https://bit.ly/1dNVPAW"
  val googleURL = "http://www.google.com/"

  // sends to https://httpstat.us/301 that sends to https://httpstat.us/
  val doubleRedirect = "https://tinyurl.com/yatcduve"
  val doubleRedirectOneHop = "https://httpstat.us/301"
  val doubleRedirectFinalDestination = "https://httpstat.us"

  "URLUnshortener" should "expand expandable URLs" in {
    val unshortener = URLUnshortener()

    unshortener.expand(expandableURL) shouldBe googleURL
    unshortener.expand(doubleRedirect) shouldBe doubleRedirectFinalDestination
  }

  "URLUnshortener" should "return the same URL if it is not expandable" in {
    val unshortener = URLUnshortener()

    unshortener.expand(googleURL) shouldBe googleURL
  }

  "URLUnshortener" should "maintain a cache of expanded URLs" in {
    val unshortener = URLUnshortener()

    unshortener.expand(expandableURL)
    unshortener.expand(googleURL)

    unshortener.cache should contain key expandableURL
    unshortener.cache should not contain googleURL

    unshortener.expand(doubleRedirect)
    unshortener.cache should contain key doubleRedirect
    unshortener.cache should contain key doubleRedirectOneHop
  }
}
