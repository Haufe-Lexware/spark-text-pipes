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

import com.haufe.umantis.ds.nlp.TransformerList
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.catalyst.ScalaReflection.schemaFor
import org.apache.spark.sql.types.DataType

import scala.collection.mutable
import scala.reflect.runtime.universe._


class UnaryUDFTransformer[T: TypeTag, U: TypeTag](
                                                   override val uid: String,
                                                   f: T => U
                                                 ) extends UnaryTransformer[T, U,
  UnaryUDFTransformer[T, U]]
  with TransformerList {

  override protected def createTransformFunc: T => U = f

  override protected def validateInputType(inputType: DataType): Unit =
    require(inputType == schemaFor[T].dataType)

  override protected def outputDataType: DataType = schemaFor[U].dataType

  override def transform(data: Seq[Map[String, Any]]): Seq[Map[String, Any]] = {
    val res = data.map(p => {
      p + (getOutputCol -> createTransformFunc(p(getInputCol).asInstanceOf[T]))
    })
    res
  }
}


// Example
object WordCounter extends Serializable {
  def counter(text: String): Int = {
    text.split(" ").length
  }

  def apply(): UnaryUDFTransformer[String, Int] = {
    new UnaryUDFTransformer(Identifiable.randomUID("WordCounter"), counter)
  }
}

object NormalizedBagOfWords {

  def transform(tokens: Seq[String]): Seq[(String, Int)] = {
    val map = new mutable.HashMap[String, Int]()
    tokens.foreach(t => {
      map.put(t, map.getOrElse(t, 0) + 1)
    })
    map.toSeq
  }

  def apply(): UnaryUDFTransformer[Seq[String], Seq[(String, Int)]] = {
    new UnaryUDFTransformer(Identifiable.randomUID("NormalizedBagOfWords"), transform)
  }
}