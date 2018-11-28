package org.apache.spark.sql

import org.apache.spark.sql.catalyst.json.JSONOptions
import org.apache.spark.sql.execution.datasources.json.TextInputJsonDataSource
import org.apache.spark.sql.types.StructType

object JsonSchemaInferrer {
  def getJsonSchema(sparkSession: SparkSession, ds: Dataset[String]): StructType = {

    val parsedOptions = new JSONOptions(
      Map[String, String](),
      sparkSession.sessionState.conf.sessionLocalTimeZone,
      sparkSession.sessionState.conf.columnNameOfCorruptRecord)

    TextInputJsonDataSource.inferFromDataset(ds, parsedOptions)
  }
}
