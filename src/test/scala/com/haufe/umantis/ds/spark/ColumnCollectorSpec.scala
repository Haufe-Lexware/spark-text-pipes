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

package com.haufe.umantis.ds.spark

import com.haufe.umantis.ds.nlp._
import com.haufe.umantis.ds.tests.SparkSpec
import org.scalatest.Matchers._


class ColumnCollectorSpec extends SparkSpec {

  val jobPositionTitle = ColnamesText("PositionTitle")
  val jobSummary = ColnamesText("summary")
  val empPositionTitle = ColnamesText("current_position_title")
  val empJobFunction = ColnamesText("job_function")

  val jobCurrentLocation = ColnamesLocation("Location")
  val empCurrentLocation = ColnamesLocation("city", Some("country"))

  val pro_empPositionTitle__mar_jobPositionTitle: ColnamesTextSimilarity =
    ColnamesTextSimilarity(empPositionTitle, jobPositionTitle)

  val pro_empJobFunction__mar_jobPositionTitle: ColnamesTextSimilarity =
    ColnamesTextSimilarity(empJobFunction, jobPositionTitle)

  val pro_empJobTitleFields__mar_jobPositionTitle =
    ColnamesAggregated(
      "mar_jobPositionTitle__pro_empJobTitleFields",
      Array(
        pro_empPositionTitle__mar_jobPositionTitle,
        pro_empJobFunction__mar_jobPositionTitle
      )
    )

  val currentLocationDistanceCols =
    ColnamesDistance(jobCurrentLocation, empCurrentLocation)
  //  val desiredLocationDistanceCols =
  // ColnamesDistance(mar.currentLocation, empsDesiredLocationCols)
  val recScoreCols = ColnamesAggregated("Score",
    Array(
      currentLocationDistanceCols,
      // desiredLocationDistanceCols,
      pro_empJobTitleFields__mar_jobPositionTitle
    ))

  val testPipeline: PipelineExtended = new DsPipeline(
    Seq(
      DsPipelineInput(
        Seq(
          pro_empPositionTitle__mar_jobPositionTitle,
          pro_empJobFunction__mar_jobPositionTitle
        ),
        StandardPipeline.TextDataScoringCentroid
      ),
      DsPipelineInput(
        pro_empJobTitleFields__mar_jobPositionTitle,
        Stg.Max
      ),
      DsPipelineInput(
        Seq(
          currentLocationDistanceCols
          // cols.desiredLocationDistanceCols
        ),
        StandardPipeline.DistanceScoring
      ),
      DsPipelineInput(
        recScoreCols,
        Stg.LinearWeigher
      )
    )
  ).pipeline

  "Search pipeline" should "have required columns" in {

    val requiredColumns = ColumnCollector(
      Seq("not_required_by_pipeline1", "not_required_by_pipeline2"),
      Seq(
        pro_empPositionTitle__mar_jobPositionTitle.baseText.vector,
        pro_empPositionTitle__mar_jobPositionTitle.baseText.language,
        pro_empJobFunction__mar_jobPositionTitle.baseText.vector,
        pro_empJobFunction__mar_jobPositionTitle.baseText.language
      ),
      testPipeline
    )

    requiredColumns.toSet shouldEqual
      Set(
        "not_required_by_pipeline1",
        "not_required_by_pipeline2",

        "PositionTitle__vector",
        "Location__score__city__mar_jobPositionTitle__pro_empJobTitleFields__linearWeights",
        "PositionTitle__language",
        "Location__coords"
      )
  }
}
