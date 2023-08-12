package me.mnedokushev.zio.apache.arrow.core.codec

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import zio._

final case class VectorSchemaRootCodec[Val](
  encoder: VectorSchemaRootEncoder[Val],
  decoder: VectorSchemaRootDecoder[Val]
) { self =>

  def transform[B](f: Val => B, g: B => Val): VectorSchemaRootCodec[B] =
    VectorSchemaRootCodec(encoder.contramap(g), decoder.map(f))

  def decodeZIO(root: VectorSchemaRoot): Task[Chunk[Val]] =
    decoder.decodeZIO(root)

  def decode(root: VectorSchemaRoot): Either[Throwable, Chunk[Val]] =
    decoder.decode(root)

  def encodeZIO(chunk: Chunk[Val], root: VectorSchemaRoot): RIO[Scope with BufferAllocator, VectorSchemaRoot] =
    encoder.encodeZIO(chunk, root)

  def encode(
    chunk: Chunk[Val],
    root: VectorSchemaRoot
  )(implicit alloc: BufferAllocator): Either[Throwable, VectorSchemaRoot] =
    encoder.encode(chunk, root)

}
