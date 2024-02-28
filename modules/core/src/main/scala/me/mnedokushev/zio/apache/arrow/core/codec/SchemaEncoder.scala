package me.mnedokushev.zio.apache.arrow.core.codec

import org.apache.arrow.vector.types.pojo.{ ArrowType, Field, FieldType, Schema => JSchema }
import zio.schema.Schema

trait SchemaEncoder[A] { self =>

  def encode(implicit schema: Schema[A]): Either[Throwable, JSchema] =
    Left(EncoderError(s"Given ZIO schema $schema mut be of type Schema.Record[A]"))

  def encodeField(name: String, nullable: Boolean): Field

}

object SchemaEncoder {

  def field(name: String, arrowType: ArrowType, nullable: Boolean): Field =
    new Field(name, new FieldType(nullable, arrowType, null), null)

  def fieldNullable(name: String, arrowType: ArrowType): Field =
    field(name, arrowType, nullable = true)

  def fieldNotNullable(name: String, arrowType: ArrowType): Field =
    field(name, arrowType, nullable = false)

}
