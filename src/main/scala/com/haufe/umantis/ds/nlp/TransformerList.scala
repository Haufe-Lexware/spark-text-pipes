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

import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.spark.ml.Pipeline
import scala.collection.mutable.ListBuffer

trait TransformerList {
  def transform(data: Seq[Map[String, Any]]): Seq[Map[String, Any]]
}

class RegexTokenizerExtended extends RegexTokenizer with TransformerList {
  def transform(data: Seq[Map[String, Any]]): Seq[Map[String, Any]] = {
    val res = data.map(p => {
      p + (getOutputCol -> createTransformFunc(p(getInputCol).asInstanceOf[String]))
    })
    res
  }

  override protected def createTransformFunc: String => Seq[String] = { originStr =>
    if (originStr == null) null else {
      val re = $(pattern).r
      val str = if ($(toLowercase)) originStr.toLowerCase() else originStr
      val tokens = if ($(gaps)) re.split(str).toSeq else re.findAllIn(str).toSeq
      val minLength = $(minTokenLength)
      tokens.filter(_.length >= minLength)
    }
  }
}

class PipelineModelWithTransformerList(protected val stages: Array[TransformerList])
  extends TransformerList  {
  def transform(data: Seq[Map[String, Any]]): Seq[Map[String, Any]] = {
    stages.foldLeft(data)((cur, transformer) => {
      transformer.transform(cur)
    })
  }
}

class PipelineExtended extends Pipeline {
   def fit(data: Seq[Map[String, Any]]): PipelineModelWithTransformerList = {
    val transformers = ListBuffer.empty[TransformerList]
    getStages.foreach {
      case (stage) =>
        val t = stage match {
          case p: TransformerList => p
          case _ =>
            throw new IllegalArgumentException(
              s"Does not support stage $stage of type ${stage.getClass} TranformerList")

        }
        transformers += t
    }
    val model = new PipelineModelWithTransformerList(transformers.toArray)
    model
  }
}