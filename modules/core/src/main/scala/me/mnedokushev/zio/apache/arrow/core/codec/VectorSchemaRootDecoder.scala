package me.mnedokushev.zio.apache.arrow.core.codec

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.complex.reader.FieldReader
import zio._
import zio.schema.DynamicValue

import scala.annotation.unused
import scala.util.control.NonFatal

trait VectorSchemaRootDecoder[+A] extends ValueDecoder[A] { self =>

  final def decodeZIO(root: VectorSchemaRoot): Task[Chunk[A]] =
    ZIO.fromEither(decode(root))

  final def decode(root: VectorSchemaRoot): Either[Throwable, Chunk[A]] =
    try
      Right(decodeUnsafe(root))
    catch {
      case decoderError: DecoderError => Left(decoderError)
      case NonFatal(ex)               => Left(DecoderError("Error decoding vector schema root", Some(ex)))
    }

  final def map[B](f: A => B): VectorSchemaRootDecoder[B] =
    new VectorSchemaRootDecoder[B] {

      override def decodeField(reader: FieldReader): DynamicValue = ???

      override def decodeValue(name: Option[String], reader: FieldReader): DynamicValue = ???

      override def decodeUnsafe(root: VectorSchemaRoot): Chunk[B] =
        self.decodeUnsafe(root).map(f)
    }

  protected def decodeUnsafe(@unused root: VectorSchemaRoot): Chunk[A] =
    throw DecoderError(s"Given ZIO schema must be of type Schema.Record[A]")

  def decodeField(reader: FieldReader): DynamicValue =
    self.decodeValue(None, reader)

}
