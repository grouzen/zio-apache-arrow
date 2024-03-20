package me.mnedokushev.zio.apache.arrow.core.codec

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
// import org.apache.arrow.vector.complex.writer.FieldWriter
import zio._

import scala.util.control.NonFatal

trait ValueVectorEncoder[-A, V <: ValueVector] extends ValueEncoder[A] { self =>

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
