package me.mnedokushev.zio.apache.arrow.core.codec

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ValueVector
import zio._

final case class ValueVectorCodec[V <: ValueVector, A](
  encoder: ValueVectorEncoder[V, A],
  decoder: ValueVectorDecoder[V, A]
) { self =>

  // def transform[B](f: A => B, g: B => A): ValueVectorCodec[B, V] =
  //   ValueVectorCodec(encoder.contramap(g), decoder.map(f))

  def decodeZIO(vec: V): Task[Chunk[A]] =
    decoder.decodeZIO(vec)

  def decode(vec: V): Either[Throwable, Chunk[A]] =
    decoder.decode(vec)

  def encodeZIO(chunk: Chunk[A]): RIO[Scope with BufferAllocator, V] =
    encoder.encodeZIO(chunk)

  def encode(chunk: Chunk[A])(implicit alloc: BufferAllocator): Either[Throwable, V] =
    encoder.encode(chunk)

}

object ValueVectorCodec {

  implicit def codec[V <: ValueVector, A](implicit
    encoder: ValueVectorEncoder[V, A],
    decoder: ValueVectorDecoder[V, A]
  ): ValueVectorCodec[V, A] =
    ValueVectorCodec[V, A](encoder, decoder)

}
