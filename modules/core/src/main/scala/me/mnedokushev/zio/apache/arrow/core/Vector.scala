package me.mnedokushev.zio.apache.arrow.core

import me.mnedokushev.zio.apache.arrow.core.codec.{ ValueVectorDecoder, ValueVectorEncoder }
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ValueVector
import zio._
import zio.stream.ZStream

object Vector {

  def fromChunk[V <: ValueVector]: FromChunkPartiallyApplied[V] =
    new FromChunkPartiallyApplied[V]

  final class FromChunkPartiallyApplied[V <: ValueVector](private val dummy: Boolean = true) extends AnyVal {
    def apply[A](chunk: Chunk[A])(implicit encoder: ValueVectorEncoder[V, A]): RIO[Scope with BufferAllocator, V] =
      encoder.encodeZIO(chunk)
  }

  def fromStream[V <: ValueVector]: FromStreamPartiallyApplied[V] =
    new FromStreamPartiallyApplied[V]

  final class FromStreamPartiallyApplied[V <: ValueVector](private val dummy: Boolean = true) extends AnyVal {
    def apply[R, A](
      stream: ZStream[R, Throwable, A]
    )(implicit encoder: ValueVectorEncoder[V, A]): RIO[R with Scope with BufferAllocator, V] =
      for {
        chunk <- stream.runCollect
        vec   <- fromChunk(chunk)
      } yield vec
  }

  def toChunk[A]: ToChunkPartiallyApplied[A] =
    new ToChunkPartiallyApplied[A]

  final class ToChunkPartiallyApplied[A](private val dummy: Boolean = true) extends AnyVal {
    def apply[V <: ValueVector](vec: V)(implicit decoder: ValueVectorDecoder[V, A]): Task[Chunk[A]] =
      decoder.decodeZIO(vec)
  }

  def toStream[A]: ToStreamPartiallyApplied[A] =
    new ToStreamPartiallyApplied[A]

  final class ToStreamPartiallyApplied[A](private val dummy: Boolean = true) extends AnyVal {
    def apply[V <: ValueVector](vec: V)(implicit decoder: ValueVectorDecoder[V, A]): ZStream[Any, Throwable, A] =
      ZStream.fromIterableZIO(decoder.decodeZIO(vec))
  }

}
