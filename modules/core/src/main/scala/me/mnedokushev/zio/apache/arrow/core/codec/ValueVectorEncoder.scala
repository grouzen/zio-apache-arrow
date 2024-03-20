package me.mnedokushev.zio.apache.arrow.core.codec

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import zio._
import zio.schema.Derive

import scala.util.control.NonFatal

trait ValueVectorEncoder[V <: ValueVector, -A] extends ValueEncoder[A] { self =>

  final def encodeZIO(chunk: Chunk[A]): RIO[Scope with BufferAllocator, V] =
    ZIO.fromAutoCloseable(
      ZIO.serviceWithZIO[BufferAllocator] { implicit alloc =>
        ZIO.fromEither(encode(chunk))
      }
    )

  final def encode(chunk: Chunk[A])(implicit alloc: BufferAllocator): Either[Throwable, V] =
    try
      Right(encodeUnsafe(chunk))
    catch {
      case encoderError: EncoderError => Left(encoderError)
      case NonFatal(ex)               => Left(EncoderError("Error encoding vector", Some(ex)))

    }

  protected def encodeUnsafe(chunk: Chunk[A])(implicit alloc: BufferAllocator): V

  // final def contramap[B](f: B => A): ValueVectorEncoder[B, V] =
  //   new ValueVectorEncoder[B, V] {
  //     override protected def encodeUnsafe(chunk: Chunk[B])(implicit alloc: BufferAllocator): V =
  //       self.encodeUnsafe(chunk.map(f))

  //     override def encodeValue(
  //       value: B,
  //       name: Option[String],
  //       writer: FieldWriter
  //     )(implicit alloc: BufferAllocator): Unit =
  //       self.encodeValue(f(value), name, writer)

  //   }

}

object ValueVectorEncoder {

  implicit val stringEncoder: ValueVectorEncoder[VarCharVector, String] =
    Derive.derive[ValueVectorEncoder[VarCharVector, *], String](ValueVectorEncoderDeriver.default[VarCharVector])
  implicit val intEncoder: ValueVectorEncoder[IntVector, Int]           =
    Derive.derive[ValueVectorEncoder[IntVector, *], Int](ValueVectorEncoderDeriver.default[IntVector])

}
