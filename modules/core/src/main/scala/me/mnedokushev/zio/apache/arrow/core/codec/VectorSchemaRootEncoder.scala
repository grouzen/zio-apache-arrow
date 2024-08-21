package me.mnedokushev.zio.apache.arrow.core.codec

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.complex.writer.FieldWriter
import org.apache.arrow.vector.{ FieldVector, VectorSchemaRoot }
import zio._
import zio.schema.{ Deriver, Factory, Schema, StandardType }

import scala.annotation.{ nowarn, unused }
import scala.util.control.NonFatal

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

  def encodeField(value: A, writer: FieldWriter)(implicit alloc: BufferAllocator): Unit

  def getWriter(vec: FieldVector): FieldWriter

  final def contramap[B](f: B => A): VectorSchemaRootEncoder[B] =
    new VectorSchemaRootEncoder[B] {

      override protected def encodeUnsafe(chunk: Chunk[B], root: VectorSchemaRoot)(implicit
        alloc: BufferAllocator
      ): VectorSchemaRoot =
        self.encodeUnsafe(chunk.map(f), root)

      override def encodeValue(value: B, name: Option[String], writer: FieldWriter)(implicit
        alloc: BufferAllocator
      ): Unit =
        self.encodeValue(f(value), name, writer)

      override def encodeField(value: B, writer: FieldWriter)(implicit alloc: BufferAllocator): Unit =
        self.encodeField(f(value), writer)

      override def getWriter(vec: FieldVector): FieldWriter =
        self.getWriter(vec)

    }

}

object VectorSchemaRootEncoder {

  def primitive[A](
    encodeValue0: (A, Option[String], FieldWriter, BufferAllocator) => Unit,
    encodeField0: (A, FieldWriter, BufferAllocator) => Unit,
    getWriter0: FieldVector => FieldWriter
  )(implicit @nowarn ev: StandardType[A]): VectorSchemaRootEncoder[A] =
    new VectorSchemaRootEncoder[A] {

      override def encodeValue(value: A, name: Option[String], writer: FieldWriter)(implicit
        alloc: BufferAllocator
      ): Unit =
        encodeValue0(value, name, writer, alloc)

      override def encodeField(value: A, writer: FieldWriter)(implicit alloc: BufferAllocator): Unit =
        encodeField0(value, writer, alloc)

      override def getWriter(vec: FieldVector): FieldWriter =
        getWriter0(vec)

    }

  implicit def encoder[A: Factory: Schema]: VectorSchemaRootEncoder[A] =
    fromDefaultDeriver[A]

  def fromDeriver[A: Factory: Schema](deriver: Deriver[VectorSchemaRootEncoder]): VectorSchemaRootEncoder[A] =
    implicitly[Factory[A]].derive[VectorSchemaRootEncoder](deriver)

  def fromDefaultDeriver[A: Factory: Schema]: VectorSchemaRootEncoder[A] =
    fromDeriver[A](VectorSchemaRootEncoderDeriver.default)

  def fromSummonedDeriver[A: Factory: Schema]: VectorSchemaRootEncoder[A] =
    fromDeriver[A](VectorSchemaRootEncoderDeriver.summoned)

}
