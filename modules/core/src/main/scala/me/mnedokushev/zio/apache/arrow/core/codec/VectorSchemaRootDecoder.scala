package me.mnedokushev.zio.apache.arrow.core.codec

import org.apache.arrow.vector.complex.reader.FieldReader
import org.apache.arrow.vector.{ ValueVector, VectorSchemaRoot }
import zio._
import zio.schema.{ Deriver, DynamicValue, Factory, Schema }

import scala.annotation.unused
import scala.util.control.NonFatal

trait VectorSchemaRootDecoder[A] extends ValueDecoder[A] { self =>

  final def decodeZIO(root: VectorSchemaRoot): Task[Chunk[A]] =
    ZIO.fromEither(decode(root))

  final def decode(root: VectorSchemaRoot): Either[Throwable, Chunk[A]] =
    try
      Right(decodeUnsafe(root))
    catch {
      case decoderError: DecoderError => Left(decoderError)
      case NonFatal(ex)               => Left(DecoderError("Error decoding vector schema root", Some(ex)))
    }

  protected def decodeUnsafe(@unused root: VectorSchemaRoot): Chunk[A] =
    throw DecoderError(s"Given ZIO schema must be of type Schema.Record[A]")

  def decodeField[V0 <: ValueVector](reader: FieldReader, vec: V0, idx: Int): DynamicValue

  final def map[B](f: A => B)(implicit schemaSrc: Schema[A], schemaDst: Schema[B]): VectorSchemaRootDecoder[B] =
    new VectorSchemaRootDecoder[B] {

      override protected def decodeUnsafe(root: VectorSchemaRoot): Chunk[B] =
        self.decodeUnsafe(root).map(f)

      override def decodeValue[V0 <: ValueVector](
        name: Option[String],
        reader: FieldReader,
        vec: V0,
        idx: Int
      ): DynamicValue =
        self
          .decodeValue(name, reader, vec, idx)
          .toValue(schemaSrc)
          .map(a => schemaDst.toDynamic(f(a)))
          .toTry
          .get

      override def decodeField[V0 <: ValueVector](reader: FieldReader, vec: V0, idx: Int): DynamicValue =
        self
          .decodeField(reader, vec, idx)
          .toValue(schemaSrc)
          .map(a => schemaDst.toDynamic(f(a)))
          .toTry
          .get

    }

}

object VectorSchemaRootDecoder {

  implicit def decoder[A: Factory: Schema]: VectorSchemaRootDecoder[A] =
    fromDefaultDeriver[A]

  def fromDeriver[A: Factory: Schema](deriver: Deriver[VectorSchemaRootDecoder]): VectorSchemaRootDecoder[A] =
    implicitly[Factory[A]].derive[VectorSchemaRootDecoder](deriver)

  def fromDefaultDeriver[A: Factory: Schema]: VectorSchemaRootDecoder[A] =
    fromDeriver[A](VectorSchemaRootDecoderDeriver.default)

  def fromSummonedDeriver[A: Factory: Schema]: VectorSchemaRootDecoder[A] =
    fromDeriver[A](VectorSchemaRootDecoderDeriver.summoned)

}
