package me.mnedokushev.zio.apache.arrow.core.codec

import zio._

trait ArrowDecoder[Vector, +Val] { self =>

  def decode(from: Vector): Either[Throwable, Chunk[Val]]

  def decodeOne(from: Vector, idx: Int): Val

  def decodeZIO(from: Vector): Task[Chunk[Val]] =
    ZIO.fromEither(decode(from))

  def flatMap[B](f: Val => ArrowDecoder[Vector, B]): ArrowDecoder[Vector, B]

  def map[B](f: Val => B): ArrowDecoder[Vector, B]

}
