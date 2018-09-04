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

import com.haufe.umantis.ds.nlp.params.HasTransliterator
import com.ibm.icu.text.Transliterator
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection.schemaFor
import org.apache.spark.sql.types.DataType

import scala.collection.mutable

object ICUTransliterators extends Serializable {

  @transient lazy val transliterators: mutable.Map[String, Transliterator] = mutable.Map()

  def getTransformer(transformation: String): Transliterator = {
    transliterators.getOrElseUpdate(transformation, Transliterator.getInstance(transformation))
  }
}


class ICUTransformer(override val uid: String)
// first arg is input, second output, third is class name
  extends UnaryTransformer[String, String, ICUTransformer] with HasTransliterator {

  def this() = this(Identifiable.randomUID("ICUTransformer"))

  setDefault(transliteratorID, "Lower; Any-Latin; Latin-ASCII")

  setDefault(additionalTransliteratorsList, List())

  val transliteratorsB: Broadcast[ICUTransliterators.type] =
    SparkSession.builder().getOrCreate().sparkContext.broadcast(ICUTransliterators)

  def transliterateText(text: String): String = {

    if (text != null) {
      // We might need to register additional transliterators here, at runtime.
      // Since Spark is distributed, a worker node Transliterator won't have any
      // user defined transliterators registered
      if (!additionalTransliteratorsAdded) {
        $(additionalTransliteratorsList).foreach(t => Transliterator.registerInstance(t))
        additionalTransliteratorsAdded = true
      }

      transliteratorsB
        .value
        .getTransformer(getTransliteratorID)
        .transliterate(text)
    } else {
      null
    }
  }

  override protected def createTransformFunc: String => String = transliterateText

  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType == schemaFor[String].dataType)
  }

  override protected def outputDataType: DataType = {
    schemaFor[String].dataType
  }
}
