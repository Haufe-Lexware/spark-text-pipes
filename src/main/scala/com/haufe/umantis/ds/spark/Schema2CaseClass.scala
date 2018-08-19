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

/**
  * Generate Case class from DataFrame.schema
  *
  *  val df:DataFrame = ...
  *
  *  val s2cc = new Schema2CaseClass
  *  import s2cc.implicits._
  *
  *  println(s2cc.schemaToCaseClass(df.schema, "MyClass"))
  *
  */

import org.apache.spark.sql.types._

class Schema2CaseClass {
  type TypeConverter = (DataType) => String

  def schemaToCaseClass(schema:StructType, className:String)(implicit tc:TypeConverter):String = {
    def genField(s:StructField):String = {
      val f = tc(s.dataType)
      s match {
        case x if(x.nullable) => s"  ${s.name}:Option[$f]"
        case _ => s"  ${s.name}:$f"
      }
    }

    val fieldsStr = schema.map(genField).mkString(",\n  ")
    s"""
       |case class $className (
       |  $fieldsStr
       |)
  """.stripMargin
  }

  object implicits {
    implicit val defaultTypeConverter:TypeConverter = (t:DataType) => { t match {
      case _:ByteType => "Byte"
      case _:ShortType => "Short"
      case _:IntegerType => "Int"
      case _:LongType => "Long"
      case _:FloatType => "Float"
      case _:DoubleType => "Double"
      case _:DecimalType => "java.math.BigDecimal"
      case _:StringType => "String"
      case _:BinaryType => "Array[Byte]"
      case _:BooleanType => "Boolean"
      case _:TimestampType => "java.sql.Timestamp"
      case _:DateType => "java.sql.Date"
      case _:ArrayType => "scala.collection.Seq"
      case _:MapType => "scala.collection.Map"
      case _:StructType => "org.apache.spark.sql.Row"
      case _ => "String"
    }}
  }
}
