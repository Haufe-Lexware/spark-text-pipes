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

package com.haufe.umantis.ds.nlp.stanford.corenlp

import org.scalatest.FlatSpec
import org.scalatest.tagobjects.Slow


class LocalCoreNLPSpec extends FlatSpec {
  def time(name: String, block: => Unit): Long = {
    val t0 = System.nanoTime()
    block // call-by-name
    val t1 = System.nanoTime()
    println(s"$name: " + (t1 - t0) / 1000000000f + "s")
    t1 - t0
  }

  "Performance in local" must "be the same" taggedAs Slow in {
    (1 to 100).foreach(x => time(s"UDF local $x", {
      val res = StanfordCoreNLPHelper.local().tokenize("en")(
        "The Senior System Center Operation " +
        "Manager (SCOM) Engineer is responsible for managing and maintaining the global " +
        "enterprise SCOM platform.")
      val exp = Seq("The", "Senior", "System", "Center", "Operation", "Manager", "-LRB-", "SCOM",
        "-RRB-", "Engineer", "is", "responsible", "for", "managing", "and", "maintaining", "the",
        "global", "enterprise", "SCOM", "platform", ".")

      assert(exp == res)
    }))
  }
}
