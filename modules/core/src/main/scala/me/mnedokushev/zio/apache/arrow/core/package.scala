package me.mnedokushev.zio.apache.arrow

import me.mnedokushev.zio.apache.arrow.core.codec.SchemaEncoder
import org.apache.arrow.vector.types.pojo.Schema
import zio.schema.{ Schema => ZSchema }
import zio._

package object core {

  def whenSchemaValid[A: ZSchema, B](schema: Schema)(ifValid: => B): B =
    // TODO: cache result of `schemaRoot` for better performance
    SchemaEncoder.schemaRoot[A] match {
      case Right(s) if s == schema => ifValid
      case Right(s)                => throw ValidationError(s"Schemas are not equal $s != $schema")
      case Left(error)             => throw error
    }

  def validateSchema[A: ZSchema](schema: Schema): Task[Unit] =
    ZIO.attempt(whenSchemaValid[A, Unit](schema)(()))

}
