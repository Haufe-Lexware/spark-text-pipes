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

package com.haufe.umantis.ds.nlp

import com.haufe.umantis.ds.spark.{DataFrameHelpers, SparkIO}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._


case class AcronymsDataSource(dataFrame: DataFrame,
                              columns: Seq[ColnamesText])


trait Acronyms extends SparkIO with DataFrameHelpers {
  def detectAcronyms(dataSource: AcronymsDataSource): DataFrame = {
    val detectUpper: (Seq[String] => Seq[String]) = (arr: Seq[String]) => {
      val acronyms = arr.flatMap(s => if (s.forall(_.isUpper)) Some(s) else None).distinct
      if (acronyms.nonEmpty) {acronyms} else null
    }
    val detectUpperUDF = udf { arr: Seq[String] => detectUpper(arr) }

    dataSource.columns.map(ds =>
      dataSource.dataFrame
        .withColumn("acronymsSeq", detectUpperUDF(col(ds.cleanWords)))
        .select("acronymsSeq", ds.language, ds.text)
        .withColumnRenamed(ds.cleanWords, "text")
        .withColumnRenamed(ds.language, "Language")
        .filter("acronymsSeq is not null")
        .withColumn("acronyms", explode(col("acronymsSeq")))
        .drop("acronymsSeq")
    )
      .reduce(_ union _)
  }

  def getAcronyms(input: Seq[AcronymsDataSource]): DataFrame = {
    input
      .map(i => detectAcronyms(i))
      .reduce(_ union _)
      .groupBy("acronyms").agg(
        collect_set("Language").alias("Languages"),
        collect_set("text").alias("texts"))
      .withColumn("expansion", lit(""))
      .sort("Languages", "acronyms")
      .select("acronyms", "expansion", "Languages", "texts") // to reorder columns
  }

  def stringifyAcronyms(df: DataFrame): DataFrame = {
    import currentSparkSession.implicits._

    val stringify = udf((vs: Seq[String]) => s"""${vs.mkString(" ||| ")}""")

    df
      .withColumn("Languages", stringify($"Languages"))
      .withColumn("texts", stringify($"texts"))
  }

  val sanitizeUDF: UserDefinedFunction = udf { text: String =>
    text
      .split("""[^\p{Ll}\p{Lu}0-9]""")
      .filter(x => x.length > 0)
      .map(_.capitalize)
      .mkString(" ")
  }

  def csvToDataFrame(inFilename: String,
                     outFilename: String = "AcronymsExpansion"): DataFrame = {
    val acronyms = readCSV(inFilename)

    val df = acronyms.withColumn("expansion", sanitizeUDF(col("expansion")))
    writeDataFrame(df, outFilename)
    df
  }
}

object Acronyms extends Acronyms
