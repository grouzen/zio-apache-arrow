package me.mnedokushev.zio.apache.arrow.core.codec

import zio._

trait ArrowDecoder[-From, +To] {

  def decodeOne(from: From, idx: Int): To

  def decode(from: From): Either[Throwable, Chunk[To]]

  def decodeZIO(from: From): Task[Chunk[To]] =
    ZIO.fromEither(decode(from))

}