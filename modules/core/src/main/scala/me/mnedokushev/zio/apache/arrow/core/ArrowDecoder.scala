package me.mnedokushev.zio.apache.arrow.core

import zio._

trait ArrowDecoder[-From, +To] {

  protected def decodeUnsafe(from: From, idx: Int): To

  def decode(from: From): Either[Throwable, Chunk[To]]

  def decodeZio(from: From): Task[Chunk[To]] =
    ZIO.fromEither(decode(from))

}
