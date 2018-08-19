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

package object nlp {    
  implicit class MapT(val underlying: Map[String,Any]) extends AnyVal {
      def getAs[T](key: String): T = underlying.apply(key).asInstanceOf[T]
  } 

  implicit class SeqFirst(val underlying: Seq[Map[String,Any]]) extends AnyVal {
      def first(): Map[String,Any] = underlying.head
  } 
}
