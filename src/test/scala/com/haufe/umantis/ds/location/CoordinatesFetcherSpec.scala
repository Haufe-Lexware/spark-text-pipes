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

import com.haufe.umantis.ds.nlp.{ColnamesLocation, DsPipeline}
import com.haufe.umantis.ds.spark.SparkSessionWrapper
import com.haufe.umantis.ds.tests.SparkSpec
import org.apache.spark.sql.DataFrame

class CoordinatesFetcherSpec extends SparkSpec with CoordinatesFetcherSpecFixture {
  "CoordinatesFetcher" must "find the right coordinates" in {
    val cf = DsPipeline.getCoordinatesFetcher(cols)

    val result = cf.transform(df)
    result.show()
  }
}

trait CoordinatesFetcherSpecFixture extends SparkSessionWrapper {
  import currentSparkSession.implicits._

  val countryCol = "country"
  val cols = ColnamesLocation("city", Some(countryCol))

  val df: DataFrame = Seq(
    ("Naples", "it"),
    ("New York", null)
  )
    .toDF(cols.locationCol, countryCol)
}