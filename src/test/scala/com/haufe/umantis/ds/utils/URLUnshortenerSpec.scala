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

  val nonExisting = "http://nonexistiningurl.nada"

  "URLUnshortener" should "expand expandable URLs" in {
    val unshortener = URLUnshortener()

    val checkedURL = unshortener.expand(expandableURL)
    checkedURL.finalUrl shouldBe googleURL
    checkedURL.origUrl shouldBe googleURL
    checkedURL.numRedirects shouldBe 1
    checkedURL.connects shouldBe true

    val doubleRedirectChecked = unshortener.expand(doubleRedirect)
    doubleRedirectChecked.finalUrl shouldBe doubleRedirectFinalDestination
    doubleRedirectChecked.origUrl shouldBe doubleRedirect
    doubleRedirectChecked.numRedirects shouldBe 2
    doubleRedirectChecked.connects shouldBe true
  }

  "URLUnshortener" should "return the same URL if it is not expandable" in {
    val unshortener = URLUnshortener()

    val googleChecked = unshortener.expand(googleURL)
    googleChecked.finalUrl shouldBe googleURL
    googleChecked.origUrl shouldBe googleURL
    googleChecked.numRedirects shouldBe 0
    googleChecked.connects shouldBe true
  }

  "URLUnshortener" should "maintain a cache of expanded URLs" in {
    val unshortener = URLUnshortener()

    unshortener.expand(expandableURL)
    unshortener.expand(googleURL)

    unshortener.cache should contain key expandableURL
    unshortener.cache should not contain googleURL

    unshortener.expand(doubleRedirect)
    unshortener.cache should contain key doubleRedirect

    // because only final urls are cached
    unshortener.cache shouldNot contain key doubleRedirectOneHop
  }

  "URLUnshortener" should "correclty deal with non-existing URLs" in {
    val unshortener = URLUnshortener()

    val nonExistingChecked = unshortener.expand(nonExisting)
    nonExistingChecked.finalUrl shouldBe nonExisting
    nonExistingChecked.origUrl shouldBe nonExisting
    nonExistingChecked.connects shouldBe false
    nonExistingChecked.numRedirects shouldBe 0
  }
}
