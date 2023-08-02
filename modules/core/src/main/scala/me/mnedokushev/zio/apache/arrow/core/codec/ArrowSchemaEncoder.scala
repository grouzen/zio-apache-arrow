package me.mnedokushev.zio.apache.arrow.core.codec

import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.{ ArrowType, Field, FieldType, Schema }
import zio.schema.{ Schema => ZSchema, StandardType }

import scala.annotation.tailrec
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

object ArrowSchemaEncoder {

  def encodeFlat[Val](implicit schema: ZSchema[Val]): Either[Throwable, Schema] = {

    @tailrec
    def encodeSchema[A](name: String, schemaField: ZSchema[A], nullable: Boolean): Field =
      schemaField match {
        case ZSchema.Primitive(standardType, _) =>
          encodePrimitive(name, standardType, nullable)
        case ZSchema.Optional(schemaOpt, _)     =>
          encodeSchema(name, schemaOpt, true)
        case _: ZSchema.Record[_]               =>
          field(name, new ArrowType.Struct, nullable)
        case ZSchema.Sequence(_, _, _, _, _)    =>
          field(name, new ArrowType.List, nullable)
        case lzy: ZSchema.Lazy[_]               =>
          encodeSchema(name, lzy.schema, nullable)
        case other                              =>
          throw ArrowEncoderError(s"Unsupported ZIO Schema type $other")
      }

    try {
      val fields = schema match {
        case record: ZSchema.Record[Val] =>
          record.fields.map { case ZSchema.Field(name, schemaField, _, _, _, _) =>
            encodeSchema(name, schemaField, nullable = false)
          }
        case _                           =>
          throw ArrowEncoderError(s"Given ZIO schema mut be of type Schema.Record[Val]")
      }

      Right(new Schema(fields.toList.asJava))
    } catch {
      case encodeError: ArrowEncoderError => Left(encodeError)
      case NonFatal(ex)                   => Left(ArrowEncoderError("Error encoding schema", Some(ex)))
    }
  }

  private def encodePrimitive[A](name: String, standardType: StandardType[A], nullable: Boolean): Field = {
    def field0(arrowType: ArrowType) =
      field(name, arrowType, nullable)

    standardType match {
      case StandardType.IntType    =>
        field0(new ArrowType.Int(32, true))
      case StandardType.LongType   =>
        field0(new ArrowType.Int(64, true))
      case StandardType.FloatType  =>
        field0(new ArrowType.FloatingPoint(FloatingPointPrecision.HALF))
      case StandardType.DoubleType =>
        field0(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))
      case StandardType.StringType =>
        field0(new ArrowType.Utf8)
      case other                   =>
        throw ArrowEncoderError(s"Unsupported ZIO Schema StandardType $other")
    }
  }

  private[codec] def field(name: String, arrowType: ArrowType, nullable: Boolean): Field =
    new Field(name, new FieldType(nullable, arrowType, null), null)

  private[codec] def fieldNullable(name: String, arrowType: ArrowType): Field =
    field(name, arrowType, nullable = true)

  private[codec] def fieldNotNullable(name: String, arrowType: ArrowType): Field =
    field(name, arrowType, nullable = false)

}
