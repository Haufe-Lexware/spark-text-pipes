package com.haufe.umantis.ds.sources.kafka

import java.io.ByteArrayOutputStream

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter}
import org.apache.avro.io.{BinaryDecoder, BinaryEncoder, DecoderFactory, EncoderFactory}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.avro._
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, DataType}

import scala.collection.JavaConverters._
import scala.collection.mutable


trait DataFrameAvroHelpers {

  private val cacheCapacity = 256

  val avroRegistryClients: mutable.Map[String, CachedSchemaRegistryClient] =
    mutable.Map[String, CachedSchemaRegistryClient]()

  def getSchemaRegistry(schemaRegistryURLs: String): CachedSchemaRegistryClient =
    avroRegistryClients
      .getOrElseUpdate(
        schemaRegistryURLs,
        {
          // Multiple URLs for HA mode.
          val urls = schemaRegistryURLs.split(",").toList.asJava
          new CachedSchemaRegistryClient(urls, cacheCapacity)
        }
      )

  implicit class DataFrameWithAvroHelpers(df: DataFrame) extends Serializable {

    def from_confluent_avro(
                             inputColumn: String,
                             outputColumn: String,
                             schemaRegistryURLs: String,
                             subject: String,
                             version: String = "latest"
                           )
    : DataFrame = {

      val schemaRegistry = getSchemaRegistry(schemaRegistryURLs)

      val schemaId =
        if (version == "latest") {
          schemaRegistry.getLatestSchemaMetadata(subject).getId
        } else {
          schemaRegistry.getSchemaMetadata(subject, version.toInt).getId
        }

      val schema = schemaRegistry.getByID(schemaId).toString
      println(schema)

      df.withColumn(outputColumn, from_avro(col(inputColumn), schema))
    }

    def to_confluent_avro(
                           schemaRegistryURLs: String,
                           subject: String,
                           outputColumn: String,
                           inputColumns: Array[String],
                           recordName: String = "topLevelRecord",
                           nameSpace: String = ""
                         )
    : DataFrame = {

      val inputCols = inputColumns.map(col)

      val schemaRegistry = getSchemaRegistry(schemaRegistryURLs)

      val schema = SchemaConverters.toAvroType(
        df.select(inputCols: _*).schema,
        recordName = recordName,
        nameSpace = nameSpace
      )
      schemaRegistry.register(subject, schema)

      val dfWithAvroCol = df.withColumn(outputColumn, to_avro(struct(inputCols: _*)))

      // dropping all inputColumns
      inputColumns.foldLeft(dfWithAvroCol)((dataframe, column) => dataframe.drop(column))
    }
  }

  def to_confluent_avro_wrapper(data: Column): Column = {
    new Column(CatalystDataToAvroWithSchemaRegistry(data.expr))
  }

  def from_avro_wrapper(data: Column, jsonFormatSchema: String): Column = {
    new Column(AvroDataToCatalystWSR(data.expr, jsonFormatSchema))
  }
}


case class CatalystDataToAvroWithSchemaRegistry(
                                                 child: Expression
                                               ) extends UnaryExpression {

  override def dataType: DataType = BinaryType

  @transient private lazy val avroType =
    SchemaConverters.toAvroType(child.dataType, child.nullable)

  @transient private lazy val serializer =
    new AvroSerializer(child.dataType, avroType, child.nullable)

  @transient private lazy val writer =
    new GenericDatumWriter[Any](avroType)

  @transient private var encoder: BinaryEncoder = _

  @transient private lazy val out = new ByteArrayOutputStream

  override def nullSafeEval(input: Any): Any = {
    println(avroType)
    out.reset()
    encoder = EncoderFactory.get().directBinaryEncoder(out, encoder)
    val avroData = serializer.serialize(input)
    writer.write(avroData, encoder)
    encoder.flush()
    out.toByteArray
  }

  override def simpleString: String = {
    s"to_avro(${child.sql}, ${child.dataType.simpleString})"
  }

  override def sql: String = {
    s"to_avro(${child.sql}, ${child.dataType.catalogString})"
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val expr = ctx.addReferenceObj("this", this)
    defineCodeGen(ctx, ev, input =>
      s"(byte[]) $expr.nullSafeEval($input)")
  }
}
