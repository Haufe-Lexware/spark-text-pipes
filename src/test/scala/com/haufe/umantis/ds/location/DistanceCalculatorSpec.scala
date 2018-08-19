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

class DistanceCalculatorSpec extends SparkSpec {
  import currentSparkSession.implicits._

  val distanceCalculator: DistanceCalculator = new DistanceCalculator()
    .setInputCol("col1")
    .setBaseLocationCol("base")
    .setOutputCol("minDist")

  "distance calculator" should "return minimum distance" in {
     val df = Seq((
       Seq(
         GeoCoordinates(48.2208286, 16.2399752), // Vienna (53km)
         GeoCoordinates(49.2022097, 16.4378759)  // Brno (120km)
       ),
       GeoCoordinates(48.1359244, 16.9758323) // base Bratislava
       )).toDF("base","col1")

     val minDistance = distanceCalculator
       .transform(df)
       .select("minDist")
       .collect()
       .head
       .getFloat(0)
     minDistance should ( be  < 100.0f )
  }
}
