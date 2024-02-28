package me.mnedokushev.zio.apache.arrow.core.codec

import org.apache.arrow.vector.types.pojo.{ Field, Schema => JSchema }
import zio.schema.Schema

trait SchemaEncoder[A] { self =>

  def encode(implicit schema: Schema[A]): Either[Throwable, JSchema] =
    Left(EncoderError(s"Given ZIO schema $schema mut be of type Schema.Record[A]"))

  def encodeField(name: String, nullable: Boolean): Field

}
