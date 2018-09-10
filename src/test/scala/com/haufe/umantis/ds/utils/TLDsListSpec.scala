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

import com.haufe.umantis.ds.tests.BaseSpec
import org.scalatest.Matchers._
import org.scalatest._

class TLDsListSpec extends BaseSpec {

  "sbt" should "create a resource with a fresh list of TLDs" in {
    val tlds = ConfigGetter.getConfigString("/tld.list")
      .split("\n")
      .toSet

    tlds should contain ("com")
    tlds should contain ("london")
  }
}