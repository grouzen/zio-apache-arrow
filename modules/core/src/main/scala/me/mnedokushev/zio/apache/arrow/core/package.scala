package me.mnedokushev.zio.apache.arrow

import me.mnedokushev.zio.apache.arrow.core.codec.SchemaEncoder
import org.apache.arrow.vector.types.pojo.Schema
import zio._
import zio.schema.{ Schema => ZSchema }

package object core {

  def whenSchemaValid[A: ZSchema, B](schema: Schema)(
    ifValid: => B
  )(implicit schemaEncoder: SchemaEncoder[A]): B =
    // TODO: cache result of `schemaRoot` for better performance
    schemaEncoder.encode match {
      case Right(s) if s == schema => ifValid
      case Right(s)                => throw ValidationError(s"Schemas are not equal $s != $schema")
      case Left(error)             => throw error
    }

  def validateSchema[A: ZSchema: SchemaEncoder](schema: Schema): Task[Unit] =
    ZIO.attempt(whenSchemaValid[A, Unit](schema)(()))

}
