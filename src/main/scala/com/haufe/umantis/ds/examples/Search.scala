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

package com.haufe.umantis.ds.examples

import com.haufe.umantis.ds.nlp._
import com.haufe.umantis.ds.spark.{DataFrameHelpers, SparkSessionWrapper}
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.functions.{desc, lit, monotonically_increasing_id}

class Search extends SearchFixture {

  // columns for the query
  val queryTextCols = ColnamesText("query")

  // columns for similarity between query and title and query and description
  val queryText__title = ColnamesTextSimilarity(queryTextCols, title)
  val queryText__description = ColnamesTextSimilarity(queryTextCols, description)

  // aggregated field for Score
  val searchTextFields =
    ColnamesAggregated("Score", Array(queryText__title, queryText__description))

  // pipeline definition for the query
  val searchQueryPipeline: PipelineExtended = DsPipeline(DsPipelineInput(
    queryTextCols,
    StandardPipeline.TextDataPreprocessing
  )).pipeline

  // pipeline definition for search
  val searchResultsPipeline: PipelineExtended =
    new DsPipeline(
      Seq(
        DsPipelineInput(
          Seq(
            queryText__title,
            queryText__description
          ),
          StandardPipeline.TextDataScoringCentroid
        ),
        DsPipelineInput(
          searchTextFields,
          Stg.Max
        )
      )
    ).pipeline


  def search(query: String): DataFrame = {

    // faster version for query, not using DataFrame
    val inputDf = Seq(Map("id" -> "0", "query" -> query))

    // processing the query through the pipeline
    val queryDf = searchQueryPipeline
      .fit(inputDf)
      .transform(inputDf)

    // getting query data
    val queryRow = queryDf.first()
    val queryVec = queryRow.getAs[DenseVector](queryTextCols.vector)
    val queryLanguage = queryRow.getAs[String](queryTextCols.language)

    products
      // setting query data for similarities calculations
      .withColumn(queryText__description.baseText.vector, lit(Literal.create(queryVec)))
      .withColumn(queryText__description.baseText.language, lit(Literal.create(queryLanguage)))
      .withColumn(queryText__title.baseText.vector, lit(Literal.create(queryVec)))
      .withColumn(queryText__title.baseText.language, lit(Literal.create(queryLanguage)))

      // executing the pipeline
      .transformWithPipeline(searchResultsPipeline)

      // sorting in descending order of Score
      .sort(desc(searchTextFields.score))
  }

  def test(): Unit = {
    search("bench").show
    search("brandy").show
    search("vino spagnolo").show
  }
}

trait SearchFixture extends SparkSessionWrapper with DataFrameHelpers {
  import currentSparkSession.implicits._

  val title = ColnamesText("title")
  val description = ColnamesText("description")
  val location = ColnamesLocation("location")

  val products: DataFrame = Seq(
    (
      "London",
      "Office chair",
      "Office chair with tilt mechanism and swivel function"
    ),
    (
      "Barcelona",
      "Silla de oficina", // Spanish
      "Silla de oficina muy comoda. Color negro"
    ),
    (
      "Napoli",
      "Sedia da ufficio", // Italian
      "La sedia di design più venduta"
    ),
    (
      "Edinburgh",
      "Sparkling wine",
      "Fine white sparkling wine from Spain"
    ),
    (
      "Bologna",
      "Vino frizzante italiano",
      "Vino frizzante italiano dei colli toscani"
    ),
    (
      "Paris",
      "Fine French cognac",
      "Incredible spirit from Franch"
    ),
    (
      "Los Angeles",
      "Ferrari Testarossa sport car",
      "An iconic Italian car for sale"
    )
  )
    // creating a DataFrame with specified columns
    .toDF(location.locationCol, title.colName, description.colName)

    // adding an id column
    .withColumn("id", monotonically_increasing_id)

    // reordering columns
    .select("id", location.locationCol, title.colName, description.colName)

    // applying a pipeline transformation
    .transformWithPipeline(
      DsPipeline(
        DsPipelineInput(
          // the sequence of columns to transform
          Seq(title, description),

          // the pipeline elements to use
          StandardPipeline.TextDataPreprocessing)
      )
        .pipeline
    )
}