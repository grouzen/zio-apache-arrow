package me.mnedokushev.zio.apache.arrow.core.codec

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import zio._

final case class VectorSchemaRootCodec[A](
  encoder: VectorSchemaRootEncoder[A],
  decoder: VectorSchemaRootDecoder[A]
) { self =>

  def transform[B](f: A => B, g: B => A): VectorSchemaRootCodec[B] =
    VectorSchemaRootCodec(encoder.contramap(g), decoder.map(f))

  def decodeZIO(root: VectorSchemaRoot): Task[Chunk[A]] =
    decoder.decodeZIO(root)

  def decode(root: VectorSchemaRoot): Either[Throwable, Chunk[A]] =
    decoder.decode(root)

  def encodeZIO(chunk: Chunk[A], root: VectorSchemaRoot): RIO[Scope with BufferAllocator, VectorSchemaRoot] =
    encoder.encodeZIO(chunk, root)

  def encode(
    chunk: Chunk[A],
    root: VectorSchemaRoot
  )(implicit alloc: BufferAllocator): Either[Throwable, VectorSchemaRoot] =
    encoder.encode(chunk, root)

}
