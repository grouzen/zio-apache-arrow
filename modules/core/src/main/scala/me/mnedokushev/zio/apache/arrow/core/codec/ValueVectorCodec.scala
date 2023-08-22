package me.mnedokushev.zio.apache.arrow.core.codec

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.complex.{ ListVector, StructVector }
import zio._
import zio.schema.Schema

final case class ValueVectorCodec[Val, Vector <: ValueVector](
  encoder: ValueVectorEncoder[Val, Vector],
  decoder: ValueVectorDecoder[Vector, Val]
) { self =>

  def transform[B](f: Val => B, g: B => Val): ValueVectorCodec[B, Vector] =
    ValueVectorCodec(encoder.contramap(g), decoder.map(f))

  def decodeZIO(vec: Vector): Task[Chunk[Val]] =
    decoder.decodeZIO(vec)

  def decode(vec: Vector): Either[Throwable, Chunk[Val]] =
    decoder.decode(vec)

  def encodeZIO(chunk: Chunk[Val]): RIO[Scope with BufferAllocator, Vector] =
    encoder.encodeZIO(chunk)

  def encode(chunk: Chunk[Val])(implicit alloc: BufferAllocator): Either[Throwable, Vector] =
    encoder.encode(chunk)

}

object ValueVectorCodec {

  def apply[Val, Vector <: ValueVector](implicit
    codec: ValueVectorCodec[Val, Vector]
  ): ValueVectorCodec[Val, Vector] =
    codec

  implicit def primitive[Val, Vector <: ValueVector](implicit schema: Schema[Val]): ValueVectorCodec[Val, Vector] =
    ValueVectorCodec(ValueVectorEncoder.primitive[Val, Vector], ValueVectorDecoder[Vector, Val])

  implicit def list[Val](implicit schema: Schema[Val]): ValueVectorCodec[Chunk[Val], ListVector] =
    ValueVectorCodec(ValueVectorEncoder.list[Val], ValueVectorDecoder.list[Val])

  implicit def struct[Val](implicit schema: Schema[Val]): ValueVectorCodec[Val, StructVector] =
    ValueVectorCodec(ValueVectorEncoder.struct[Val], ValueVectorDecoder.struct[Val])

}
