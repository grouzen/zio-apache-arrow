package me.mnedokushev.zio.apache.arrow.core.codec

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ValueVector
import zio._
// import zio.schema.Schema

final case class ValueVectorCodec[A, V <: ValueVector](
  encoder: ValueVectorEncoder[A, V],
  decoder: ValueVectorDecoder[V, A]
) { self =>

  def transform[B](f: A => B, g: B => A): ValueVectorCodec[B, V] =
    ValueVectorCodec(encoder.contramap(g), decoder.map(f))

  def decodeZIO(vec: V): Task[Chunk[A]] =
    decoder.decodeZIO(vec)

  def decode(vec: V): Either[Throwable, Chunk[A]] =
    decoder.decode(vec)

  def encodeZIO(chunk: Chunk[A]): RIO[Scope with BufferAllocator, V] =
    encoder.encodeZIO(chunk)

  def encode(chunk: Chunk[A])(implicit alloc: BufferAllocator): Either[Throwable, V] =
    encoder.encode(chunk)

}

// object ValueVectorCodec {

//   def apply[A, V <: ValueVector](implicit codec: ValueVectorCodec[A, V]): ValueVectorCodec[A, V] =
//     codec

//   implicit def primitive[A, V <: ValueVector](implicit schema: Schema[A]): ValueVectorCodec[A, V] =
//     ValueVectorCodec(ValueVectorEncoder.primitive[A, V], ValueVectorDecoder[V, A])

//   implicit def list[A](implicit schema: Schema[A]): ValueVectorCodec[Chunk[A], ListVector] =
//     ValueVectorCodec(ValueVectorEncoder.list[A], ValueVectorDecoder.list[A])

//   implicit def struct[A](implicit schema: Schema[A]): ValueVectorCodec[A, StructVector] =
//     ValueVectorCodec(ValueVectorEncoder.struct[A], ValueVectorDecoder.struct[A])

// }
