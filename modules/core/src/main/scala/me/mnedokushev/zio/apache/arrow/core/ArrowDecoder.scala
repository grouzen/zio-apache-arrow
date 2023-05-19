package me.mnedokushev.zio.apache.arrow.core

import zio.Chunk

trait ArrowDecoder[-From, +To] {

  protected def decodeUnsafe(from: From, idx: Int): To

  def decode(from: From): Either[Throwable, Chunk[To]]

}
