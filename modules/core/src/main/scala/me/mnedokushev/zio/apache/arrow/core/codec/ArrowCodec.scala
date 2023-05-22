package me.mnedokushev.zio.apache.arrow.core.codec

trait ArrowCodec[Vector <: AutoCloseable, Val] extends ArrowEncoder[Val, Vector] with ArrowDecoder[Vector, Val] {}
