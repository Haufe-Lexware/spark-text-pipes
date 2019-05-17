package org.apache.spark.sql.avro

import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.io.{BinaryDecoder, DecoderFactory}

import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode}
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, DataType}

case class ConfluentAvroDataToCatalyst(child: Expression, jsonFormatSchema: String)
  extends UnaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)

  override lazy val dataType: DataType = SchemaConverters.toSqlType(avroSchema).dataType

  override def nullable: Boolean = true

  @transient private lazy val avroSchema = new Schema.Parser().parse(jsonFormatSchema)

  @transient private lazy val reader = new GenericDatumReader[Any](avroSchema)

  @transient private lazy val deserializer = new AvroDeserializer(avroSchema, dataType)

  @transient private var decoder: BinaryDecoder = _

  @transient private var result: Any = _

  override def nullSafeEval(input: Any): Any = {
    val binary = input.asInstanceOf[Array[Byte]]
    decoder = DecoderFactory.get().binaryDecoder(
      binary, 1 + 4, binary.length - 1 - 4, decoder)
    result = reader.read(result, decoder)
    deserializer.deserialize(result)
  }

  override def prettyName: String = "from_confluent_avro_helper"

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val expr = ctx.addReferenceObj("this", this)
    defineCodeGen(ctx, ev, input =>
      s"(${CodeGenerator.boxedType(dataType)})$expr.nullSafeEval($input)")
  }
}
