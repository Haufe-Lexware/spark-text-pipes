package com.haufe.umantis.ds.sources.kafka.serde

import com.haufe.umantis.ds.spark.SparkIO
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.{DataFrame, JsonSchemaInferrer}


trait DataFrameJsonHelpers extends SparkIO {

  implicit class DataFrameWithJsonHelpers(df: DataFrame) extends Serializable {
    def expand_json(inputColumn: String, outputColumn: Option[String] = None): DataFrame = {
      import currentSparkSession.implicits._

      val outputCol = outputColumn match {
        case Some(c) => c
        case _ => inputColumn
      }

      val ds = df.select(inputColumn).as[String]
      val schema = JsonSchemaInferrer.getJsonSchema(currentSparkSession, ds)
      df.withColumn(outputCol, from_json(col(inputColumn), schema))
    }
  }
}

