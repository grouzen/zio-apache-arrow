package me.mnedokushev.zio.apache.arrow.core.vector

final case class VectorError(
  message: String,
  cause: Option[Throwable] = None
) extends IllegalStateException(message, cause.getOrElse(new IllegalStateException()))
