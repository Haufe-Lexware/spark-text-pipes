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

package com.haufe.umantis.ds.location

import com.haufe.umantis.ds.tests.SparkSpec
import org.scalatest.Matchers._

class DistanceScorerSpec extends SparkSpec {
  import currentSparkSession.implicits._

  val distanceScorer: DistanceScorer = new DistanceScorer()
    .setInputCol("col1")
    .setDistanceFactorCol("factor")
    .setOutputCol("score")


  "distance scorer" should "handle null" in {
    // TODO
    val df = Seq((
      0.01f,
      null
    )).toDF("base","col1")

    val minDistance = distanceScorer
      .transform(df)
      .select("score")
      .collect()
      .head
      .isNullAt(0)

    minDistance shouldEqual true

  }
}
