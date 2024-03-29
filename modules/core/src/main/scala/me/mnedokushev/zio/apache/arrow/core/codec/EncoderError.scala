package me.mnedokushev.zio.apache.arrow.core.codec

import java.io.IOException

final case class EncoderError(
  message: String,
  cause: Option[Throwable] = None
) extends IOException(message, cause.getOrElse(new Throwable()))
