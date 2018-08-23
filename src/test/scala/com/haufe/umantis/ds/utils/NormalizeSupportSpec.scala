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

class NormalizeSupportSpec extends BaseSpec with NormalizeSupport {

  "NormalizeSupport" should "correctly normalize non -ASCII characters" in {
    normalizeMore("ÀÁÂÃĀĂȦÄẢÅǍȀȂĄẠḀẦẤàáâä") shouldBe "aaaaaaaaaaaaaaaaaaaaaa"
    normalizeMore("ÉÊẼĒĔËȆȄȨĖèéêẽēȅë") shouldBe "eeeeeeeeeeeeeeeee"
    normalizeMore("ÌÍÏïØøÒÖÔöÜüŇñÇçß") shouldBe "iiiioooooouunnccss"
  }

  it should "normalize 's to nothing" in {
    normalizeMore("aa'sbba") shouldBe "aabba"
  }

  it should "normalize & for -" in {
    normalizeMore("aa & bb") shouldBe "aa-bb"
    normalizeMore("aa&& & &&& bb") shouldBe "aa-bb"
  }

  it should "normalize brackets to -" in {
    normalizeMore("aa(bb)cc") shouldBe "aa-bb-cc"
    normalizeMore("aa((((bb)))cc") shouldBe "aa-bb-cc"
  }

  it should "normalize multiples of '-' to a single '-'" in {
    normalizeMore("a----a--b-b-------a") shouldBe "a-a-b-b-a"
  }

  it should "normalize to lowercase" in {
    normalizeMore("AAbAbbB") shouldBe "aababbb"
  }

  it should "normalize a string with several diacritical marks" in {
    normalizeMore("a'sa((%%$ & b___--BB a") shouldBe "aa-b-bb-a"
  }
}