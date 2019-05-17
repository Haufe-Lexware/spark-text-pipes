package org.apache.spark.sql.avro

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.io.{BinaryEncoder, EncoderFactory}
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.{BinaryType, DataType}

case class CatalystDataToConfluentAvro(child: Expression, schemaId: Int) extends UnaryExpression {

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
    out.reset()
    out.write(0)
    out.write(ByteBuffer.allocate(4).putInt(schemaId).array)
    encoder = EncoderFactory.get().directBinaryEncoder(out, encoder)
    val avroData = serializer.serialize(input)
    writer.write(avroData, encoder)
    encoder.flush()
    out.toByteArray
  }

  override def prettyName: String = "to_confluent_avro_helper"

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val expr = ctx.addReferenceObj("this", this)
    defineCodeGen(ctx, ev, input =>
      s"(byte[]) $expr.nullSafeEval($input)")
  }
}
