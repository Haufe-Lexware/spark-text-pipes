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

package com.haufe.umantis.ds

import java.util
import java.util.Collections

import scala.collection.convert.Wrappers.JMapWrapper
import scala.ref.WeakReference


package object utils {
  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) / 1000000000f + "s")
    result
  }

  private val weakHashMap = new JMapWrapper[String, WeakReference[String]](
    Collections.synchronizedMap(
      new util.WeakHashMap[String, WeakReference[String]](100000)))

  /**
    * Implicit class to be sure that we have just 1 string in VMs.
    * We use [[util.WeakHashMap]] with [[WeakReference]]
    * so that not referenced strings can be garbage collected
    *
    * @param s The string to intern
    */
  implicit class ManualIntern(s: String) {
    def manualIntern(): String = {

      weakHashMap
        // retrieve weakreference form weak hasmap could be null then we updated
        .getOrElseUpdate(s, WeakReference(s))
        // retrieve if weakreference contains any string ref
        .get
        // if we dont have weakreference we create new one
        .getOrElse({
          weakHashMap.put(s, WeakReference(s))
          s
        })
    }
  }

  /**
    * Able create map with numeric and string types from a string. Example:
    * <code>
    * val s = "en,1.3;de,1.17"
    * val m = s.toMapOfTypes[String, Float]()
    * </code>
    *
    * @param inputString The string to create the map from.
    */
  implicit class StringToMap(inputString: String) {
    import scala.reflect._

    val StringClass: ClassTag[String] = classTag[String]
    val IntClass: ClassTag[Int] = classTag[Int]
    val DoubleClass: ClassTag[Double] = classTag[Double]
    val FloatClass: ClassTag[Float] = classTag[Float]
    val LongClass: ClassTag[Long] = classTag[Long]

    def toMapOfTypes[K, V](
                            outerSeparator: String = ";",
                            innerSeparator: String = ","
                          )(implicit tag1: ClassTag[K], tag2: ClassTag[V])
    : Map[K, V] = {

      def cast[T](x: String)(implicit tag: ClassTag[T]): T =
        tag match {
          case StringClass => x.asInstanceOf[T]
          case IntClass => x.toInt.asInstanceOf[T]
          case DoubleClass => x.toDouble.asInstanceOf[T]
          case FloatClass => x.toFloat.asInstanceOf[T]
          case LongClass => x.toLong.asInstanceOf[T]
        }

      inputString
        .split(outerSeparator)
        .map(_.split(innerSeparator).toList)
        .flatMap{
          case List(k: String, v: String) => Some(cast[K](k) -> cast[V](v))
          case _ => None
        }
        .toMap[K, V]
    }
  }
}