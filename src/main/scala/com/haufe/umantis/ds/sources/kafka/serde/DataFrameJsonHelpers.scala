package com.haufe.umantis.ds.sources.kafka.serde

import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.{DataFrame, JsonSchemaInferrer}


trait DataFrameJsonHelpers {

  implicit class DataFrameWithJsonHelpers(df: DataFrame) extends Serializable {
    def fromInferredJson(inputColumn: String, outputColumn: Option[String] = None): DataFrame = {
      import df.sparkSession.implicits._

      val outputCol = outputColumn match {
        case Some(c) => c
        case _ => inputColumn
      }

      val ds = df.select(inputColumn).as[String]
      val schema = JsonSchemaInferrer.getJsonSchema(df.sparkSession, ds)
      df.withColumn(outputCol, from_json(col(inputColumn), schema))
    }
  }
}

