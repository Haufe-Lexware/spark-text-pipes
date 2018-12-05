package org.apache.spark.sql.avro

import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.io.{BinaryDecoder, DecoderFactory}
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, DataType}


case class AvroDataToCatalystWSR(child: Expression, jsonFormatSchema: String)
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
    decoder = DecoderFactory.get().binaryDecoder(binary, 0, binary.length, decoder)
    result = reader.read(result, decoder)
    println(s"result = $result")
    val r = deserializer.deserialize(result)
    println(s"r = $r")
    r
  }

  override def simpleString: String = {
    s"from_avro(${child.sql}, ${dataType.simpleString})"
  }

  override def sql: String = {
    s"from_avro(${child.sql}, ${dataType.catalogString})"
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val expr = ctx.addReferenceObj("this", this)
    defineCodeGen(ctx, ev, input =>
      s"(${CodeGenerator.boxedType(dataType)})$expr.nullSafeEval($input)")
  }
}