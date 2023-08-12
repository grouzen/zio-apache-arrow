package me.mnedokushev.zio.apache.arrow.core

final case class ValidationError(
  message: String,
  cause: Option[Throwable] = None
) extends IllegalArgumentException(message, cause.getOrElse(new Throwable()))
