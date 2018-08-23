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

import com.haufe.umantis.ds.nlp.params.{ProvidedColumns, RequiredColumns}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.param.Params
import org.apache.spark.ml.param.shared.{HasOutputCol, HasOutputCols}

/**
  * ColumnCollector is used to select the minimum required columns
  * that are required by the pipeline. This is done for performance reasons
  * (i.e. the pipeline will run faster!)
  */
object ColumnCollector {

  implicit class AutomaticRequiredColumns(x: Params) extends RequiredColumns {
    override def requiredColumns: Seq[String] = {
      x.params.filter(p =>
        (p.name.endsWith("Col") || p.name.endsWith("Cols"))
        && p.name != "outputCol"
        && p.name != "outputCols"
      )
        .flatMap(x.get(_) match {
          case Some(s: String) => Seq(s)
          case Some(s: Seq[String]) => s
          case _ => Seq()
        })
    }
  }

  /**
    * Giving information about which columns need to be there,
    * will be provided, and the ones that will be added by the pipeline
    *
    * @param required Input columns that are NOT needed by the pipeline but
    *                 I want to have in output anyway.
    *                 This includes columns that will be outputted by the pipeline.
    * @param provided Columns that will be added manually (using .withColumn()) after the .select()
    * @param pipeline This is the pipeline I want to execute over the dataframe
    * @return The minimum sequence of required columns by the pipeline and business logic.
    */
  def apply(required: Seq[String], provided: Seq[String], pipeline: Pipeline)
  : Seq[String] = {

    val requiredInputs = pipeline
      .getStages.
      foldLeft(required) {
        (acc, stage) =>
          stage match {
            case p: RequiredColumns => acc ++ p.requiredColumns
            case p: Params => acc ++ p.requiredColumns
            case _ => throw new IllegalArgumentException(
              s"Unable determine input columns for stage $stage")
          }
    }

    val producedOutput = pipeline
      .getStages
      .foldLeft(provided: Seq[String]) {
        (acc, stage) =>
          stage match {
            case p: ProvidedColumns => acc ++ p.providedColumns
            case p: HasOutputCol => acc :+ p.getOutputCol
            case p: HasOutputCols => acc ++ p.getOutputCols
            case _ => throw new IllegalArgumentException(
              s"Unable determine output columns for stage $stage")
          }
      }

    requiredInputs.filterNot(producedOutput.contains(_)).distinct
  }
}
