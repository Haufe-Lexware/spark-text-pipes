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

import org.apache.spark.ml.Pipeline
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, StringType, StructType}

import scala.util.Try


trait DataFrameHelpers extends SparkSessionWrapper {

  implicit class DataFrameWithHelpers(df: DataFrame) extends Serializable {
    // use this as
    // df.select(flattenSchema(df.schema):_*)
    private def flattenSchema(schema: StructType, prefix: String = null): Array[Column] = {
      schema.fields.flatMap(f => {
        val colName = if (prefix == null) f.name else prefix + "." + f.name

        f.dataType match {
          case st: StructType => flattenSchema(st, colName)
          case _ => Array(col(colName))
        }
      })
    }

    def flatten(prefix: String = null): DataFrame = {
      df.select(flattenSchema(df.schema, prefix):_*)
    }

    def emptyDfWithSameSchema: DataFrame = {
      currentSparkSession.createDataFrame(currentSparkSession.sparkContext.emptyRDD[Row], df.schema)
    }

    def sanitizeColumnNames: DataFrame = {
      val cleanColNames = df.columns.map(c => """[^\p{Ll}\p{Lu}0-9]""".r.replaceAllIn(c, ""))
      df.toDF(cleanColNames:_*)
    }

    private val intToStringUDF: UserDefinedFunction = udf({ x: Int => x.toString })
    private val stringToDoubleUDF: UserDefinedFunction = udf({ x: String => x.toDouble })
    private val byteArrayToStringUDF: UserDefinedFunction =
      udf((payload: Array[Byte]) => Try(new String(payload)).getOrElse(null))

    def intToString(column: String): DataFrame = {
      df.withColumn(column, intToStringUDF(col(column)))
    }

    def stringToDouble(column: String): DataFrame = {
      df.withColumn(column, stringToDoubleUDF(col(column)))
    }

    def byteArrayToString(column: String): DataFrame = {
      df.withColumn(column, byteArrayToStringUDF(col(column)))
    }

    def deserializeAvro(column: String, deserializingFunction: UserDefinedFunction): DataFrame = {
      df.withColumn(column, deserializingFunction(col(column)))
    }

    def allNullOrEmptyStringsToDot: DataFrame = {
      val safeString: String => String = s => if (s == null || s == "") "." else s
      val udfSafeString = udf(safeString)

      val safeCols = df.schema.map(field =>
        if (field.dataType == StringType) udfSafeString(col(field.name)).alias(field.name)
        else col(field.name))

      df.select(safeCols:_*)
    }

    def toSeqOfMaps: Seq[Map[String, Any]] = {
      df
        .collect()
        .map(row => row.getValuesMap[Any](row.schema.fieldNames))
    }

    def limitIfNonZero(limit: Int): DataFrame = {
      if (limit == 0)
        df
      else
        df.limit(limit)
    }

    def transformWithPipeline[P <: Pipeline](pipeline: P): DataFrame = {
      pipeline
        .fit(df)
        .transform(df)
    }

    def toJsonString: String = {
      df.toJSON.collect.mkString("[", "," , "]")
    }

    def prefixColumnNames(prefix: String, exclusions: Set[String] = Set()): DataFrame = {
      df.toDF(df.columns.map(c => {if (exclusions.contains(c)) c else s"${prefix}__$c"}):_*)
    }

    def originalColumns(): DataFrame = {
      val columnsToShow = df.columns.filter(name => ! name.contains("__")).map(col)
      df.select(columnsToShow:_*)
    }

    /**
      * Add nested field to DataFrame
      *
      * @param newColName Dot-separated nested field name
      * @param newCol New column value
      */
    def withNestedColumn(newColName: String, newCol: Column): DataFrame = {
      DataFrameHelpers.addNestedColumn(df, newColName, newCol)
    }
  }

  def nullableCol(parentCol: Column, c: Column): Column = {
    when(parentCol.isNotNull, c)
  }

  def nullableCol(c: Column): Column = {
    nullableCol(c, c)
  }

  def createNestedStructs(splitted: Seq[String], newCol: Column): Column = {
    splitted
      .foldRight(newCol) {
        case (colName, nestedStruct) => nullableCol(struct(nestedStruct as colName))
      }
  }

  def recursiveAddNestedColumn(
                                splitted: Seq[String],
                                col: Column,
                                colType: DataType,
                                nullable: Boolean,
                                newCol: Column
                              )
  : Column = {
    colType match {
      case colType: StructType if splitted.nonEmpty => {
        var modifiedFields: Seq[(String, Column)] = colType.fields
          .map(f => {
            var curCol = col.getField(f.name)
            if (f.name == splitted.head) {
              curCol = recursiveAddNestedColumn(
                splitted.tail, curCol, f.dataType, f.nullable, newCol)
            }
            (f.name, curCol as f.name)
          })

        if (!modifiedFields.exists(_._1 == splitted.head)) {
          modifiedFields :+= (
            splitted.head,
            nullableCol(col, createNestedStructs(splitted.tail, newCol)) as splitted.head
          )
        }

        var modifiedStruct: Column = struct(modifiedFields.map(_._2): _*)
        if (nullable) {
          modifiedStruct = nullableCol(col, modifiedStruct)
        }
        modifiedStruct
      }
      case _  => createNestedStructs(splitted, newCol)
    }
  }

  def addNestedColumn(df: DataFrame, newColName: String, newCol: Column): DataFrame = {
    if (newColName.contains('.')) {
      var splitted = newColName.split('.')

      val modifiedOrAdded: (String, Column) = df.schema.fields
        .find(_.name == splitted.head)
        .map(f => (
          f.name,
          recursiveAddNestedColumn(splitted.tail, col(f.name), f.dataType, f.nullable, newCol)
        ))
        .getOrElse {
          (splitted.head, createNestedStructs(splitted.tail, newCol) as splitted.head)
        }

      df.withColumn(modifiedOrAdded._1, modifiedOrAdded._2)

    } else {
      // Top level addition, use spark method as-is
      df.withColumn(newColName, newCol)
    }
  }

  def mergeDataframes(dataframes: Seq[DataFrame]): DataFrame = {
    dataframes
      .map(df => df.withColumn("id", monotonically_increasing_id()))
      .reduce(_.join(_, Seq("id")))
      .drop("id")
  }
}

object DataFrameHelpers extends DataFrameHelpers
