package me.mnedokushev.zio.apache.arrow.core.codec

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.complex.writer.FieldWriter
import org.apache.arrow.vector.{ FieldVector, VectorSchemaRoot }
import zio._
import zio.schema.Factory

import scala.annotation.unused
import scala.util.control.NonFatal
import zio.schema.Schema
import zio.schema.Deriver

trait VectorSchemaRootEncoder[-A] extends ValueEncoder[A] { self =>

  final def encodeZIO(chunk: Chunk[A], root: VectorSchemaRoot): RIO[Scope with BufferAllocator, VectorSchemaRoot] =
    ZIO.fromAutoCloseable(
      ZIO.serviceWithZIO[BufferAllocator] { implicit alloc =>
        ZIO.fromEither(encode(chunk, root))
      }
    )

  final def encode(
    chunk: Chunk[A],
    root: VectorSchemaRoot
  )(implicit alloc: BufferAllocator): Either[Throwable, VectorSchemaRoot] =
    try
      Right(encodeUnsafe(chunk, root))
    catch {
      case encoderError: EncoderError => Left(encoderError)
      case NonFatal(ex)               => Left(EncoderError("Error encoding vector schema root", Some(ex)))
    }

  protected def encodeUnsafe(
    @unused chunk: Chunk[A],
    @unused root: VectorSchemaRoot
  )(implicit @unused alloc: BufferAllocator): VectorSchemaRoot =
    throw EncoderError(s"Given ZIO schema must be of type Schema.Record[A]")

  def encodeField(@unused vec: FieldVector, writer: FieldWriter, value: A, @unused idx: Int)(implicit
    alloc: BufferAllocator
  ): Unit =
    self.encodeValue(value, None, writer)

  // final def contramap[B](f: B => A): VectorSchemaRootEncoder[B] =
  //   new VectorSchemaRootEncoder[B] {

  //     override def encodeValue(
  //       value: B,
  //       name: Option[String],
  //       writer: FieldWriter
  //     )(implicit alloc: BufferAllocator): Unit =
  //       self.encodeValue(f(value), name, writer)

  //     override protected def encodeUnsafe(chunk: Chunk[B], root: VectorSchemaRoot)(implicit
  //       alloc: BufferAllocator
  //     ): VectorSchemaRoot =
  //       self.encodeUnsafe(chunk.map(f), root)
  //   }

}

object VectorSchemaRootEncoder {

  implicit def apply[A: Factory: Schema]: VectorSchemaRootEncoder[A] = 
    fromSummonedDeriver[A]

  def fromDeriver[A: Factory: Schema](deriver: Deriver[VectorSchemaRootEncoder]): VectorSchemaRootEncoder[A] =
    implicitly[Factory[A]].derive[VectorSchemaRootEncoder](deriver)

  def fromDefaultDeriver[A: Factory: Schema]: VectorSchemaRootEncoder[A] =
    fromDeriver[A](VectorSchemaRootEncoderDeriver.default)

  def fromSummonedDeriver[A: Factory: Schema]: VectorSchemaRootEncoder[A] =
    fromDeriver[A](VectorSchemaRootEncoderDeriver.summoned)

}
