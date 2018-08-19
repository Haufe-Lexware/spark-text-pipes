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

package com.haufe.umantis.ds.wmd

import java.io._

import scala.io.Codec
import com.haufe.umantis.ds.spark._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.haufe.umantis.ds.nlp._



object News20Dataset extends SparkIO with DataFrameHelpers {
  import currentSparkSession.implicits._

  val cols = new ColnamesText("text")

  val basePath = "word_mover_distance_datasets/20news/"

  def convertToParquet(): Unit = {
    val (trainRaw, testRaw) = datasetsFromDir()

    val train = processSet(trainRaw)
    val test = processSet(testRaw)

    writeDatasets(train, test)
  }

  def datasetsFromDir(dir: Option[String] = None): (DataFrame, DataFrame) = {
    val path = dir match {
      case Some(d) => d
      case _ => basePath + "original_format"
    }

    val datasetFiles: DirectoryContent = getListOfFiles(path)

    val trainDir: File = datasetFiles.directories.filter(f => f.getName.contains("train")).head
    val testDir: File = datasetFiles.directories.filter(f => f.getName.contains("test")).head

    (dirToDataFrame(trainDir), dirToDataFrame(testDir))
  }

  def dirToDataFrame(dir: File): DataFrame = {
    val newsDirs = getListOfFiles(dir).directories

    newsDirs
      .flatMap(group => {
        // println(s"#### ${group.getName}")
        getListOfFiles(group).files
          .map(message => {
            // print(s"## ${message.getName}")
            val decoder = Codec.ISO8859.decoder
            val source = scala.io.Source.fromFile(message)(decoder)
            val allText = try source.mkString finally source.close()
            // println(s" ${message.length()}")

            (group.getName, message.getName, allText)
          })
      })
      .toDF("group", "message", cols.text)
      .repartition(16)
  }

  def writeDatasets(train: DataFrame, test: DataFrame, dir: Option[String] = None): Unit = {
    val path = dir match {
      case Some(d) => d
      case _ => basePath
    }

    writeDataFrame(train, path + "train")
    writeDataFrame(test, path + "test")
  }

  def processSet(df: DataFrame): DataFrame = {
    val pipeline = new DsPipeline(Seq(
      DsPipelineInput(
        Seq(
          cols
        ),
        Seq(
          Stg.TextCleaner,
          Stg.Tokenizer,
          Stg.StopWordsRemover,
          Stg.EmbeddingsModel,
          Stg.NormalizedBagOfWords
        )
      )
    )).pipeline

    df
      .withColumn(cols.language, lit("en")) // we know it's all in English
      .transformWithPipeline(pipeline)
  }
}
