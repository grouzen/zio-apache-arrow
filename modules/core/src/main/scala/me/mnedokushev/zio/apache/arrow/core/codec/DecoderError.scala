package me.mnedokushev.zio.apache.arrow.core.codec

import java.io.IOException

final case class DecoderError(
  message: String,
  cause: Throwable
) extends IOException(message, cause)
