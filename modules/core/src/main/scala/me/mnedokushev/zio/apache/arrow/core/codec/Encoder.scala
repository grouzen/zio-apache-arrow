package me.mnedokushev.zio.apache.arrow.core.codec

import org.apache.arrow.memory.BufferAllocator
import zio._

trait Encoder[-Val, Vector <: AutoCloseable] {

  def encode(chunk: Chunk[Val])(implicit alloc: BufferAllocator): Either[Throwable, Vector]

  def encodeZIO(chunk: Chunk[Val]): RIO[Scope with BufferAllocator, Vector] =
    ZIO.fromAutoCloseable(
      ZIO.serviceWithZIO[BufferAllocator] { implicit alloc =>
        ZIO.fromEither(encode(chunk))
      }
    )

}
