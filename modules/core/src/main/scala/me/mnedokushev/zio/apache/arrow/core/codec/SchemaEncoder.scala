package me.mnedokushev.zio.apache.arrow.core.codec

import org.apache.arrow.vector.types.pojo.{ ArrowType, Field, FieldType, Schema => JSchema }
import zio.schema.Schema
import zio.schema.Factory
import zio.schema.Deriver
import scala.jdk.CollectionConverters._

trait SchemaEncoder[A] { self =>

  def encode(implicit schema: Schema[A]): Either[Throwable, JSchema] =
    Left(EncoderError(s"Given ZIO schema $schema mut be of type Schema.Record[A]"))

  def encodeField(name: String, nullable: Boolean): Field

}

object SchemaEncoder {

  implicit def encoder[A: Factory: Schema]: SchemaEncoder[A] =
    fromDefaultDeriver[A]

  def fromDeriver[A: Factory: Schema](deriver: Deriver[SchemaEncoder]): SchemaEncoder[A] =
    implicitly[Factory[A]].derive[SchemaEncoder](deriver)

  def fromDefaultDeriver[A: Factory: Schema]: SchemaEncoder[A] =
    fromDeriver(SchemaEncoderDeriver.default)

  def fromSummonedDeriver[A: Factory: Schema]: SchemaEncoder[A] =
    fromDeriver(SchemaEncoderDeriver.summoned)

  def primitive(name: String, tpe: ArrowType.PrimitiveType, nullable: Boolean): Field =
    new Field(name, new FieldType(nullable, tpe, null), null)

  def list(name: String, child: Field, nullable: Boolean): Field =
    new Field(name, new FieldType(nullable, new ArrowType.List, null), List(child).asJava)

  def struct(name: String, fields: List[Field], nullable: Boolean): Field =
    new Field(name, new FieldType(nullable, new ArrowType.Struct, null), fields.asJava)

}
