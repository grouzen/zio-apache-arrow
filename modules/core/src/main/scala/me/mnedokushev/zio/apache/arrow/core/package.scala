package me.mnedokushev.zio.apache.arrow

import me.mnedokushev.zio.apache.arrow.core.codec.SchemaEncoder
import org.apache.arrow.vector.types.pojo.Schema
import zio.schema.{ Schema => ZSchema }

package object core {

  def validateSchema[A, B](schema: Schema)(ifValid: => B)(implicit zschema: ZSchema[A]): B =
    // TODO: cache result of `schemaRoot` for better performance
    SchemaEncoder.schemaRoot[A] match {
      case Right(s) if s == schema => ifValid
      case Right(s)                => throw ValidationError(s"Schemas are not equal $s != $schema")
      case Left(error)             => throw error
    }

}
