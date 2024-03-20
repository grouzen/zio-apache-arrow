package me.mnedokushev.zio.apache.arrow.core.codec

import org.apache.arrow.vector._
// import org.apache.arrow.vector.complex.reader.FieldReader
import zio._
// import zio.schema.DynamicValue

import scala.util.control.NonFatal

trait ValueVectorDecoder[V <: ValueVector, +A] extends ValueDecoder[A] { self =>

  final def decodeZIO(vec: V): Task[Chunk[A]] =
    ZIO.fromEither(decode(vec))

  final def decode(vec: V): Either[Throwable, Chunk[A]] =
    try
      Right(decodeUnsafe(vec))
    catch {
      case decoderError: DecoderError => Left(decoderError)
      case NonFatal(ex)               => Left(DecoderError("Error decoding vector", Some(ex)))
    }

  protected def decodeUnsafe(vec: V): Chunk[A]

  // final def map[B](f: A => B): ValueVectorDecoder[V, B] =
  //   new ValueVectorDecoder[V, B] {

  //     // TODO: how to convert to B
  //     override def decodeValue(name: Option[String], reader: FieldReader): DynamicValue = ???

  //     override protected def decodeUnsafe(vec: V): Chunk[B] =
  //       self.decodeUnsafe(vec).map(f)
  //   }

}
