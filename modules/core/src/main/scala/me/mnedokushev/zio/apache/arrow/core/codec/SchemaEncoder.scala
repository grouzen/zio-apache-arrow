package me.mnedokushev.zio.apache.arrow.core.codec

import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.{ ArrowType, Field, FieldType, Schema }
import zio.schema.{ Schema => ZSchema, StandardType }

import scala.annotation.tailrec
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

object SchemaEncoder {

  def schemaRoot[A](implicit schema: ZSchema[A]): Either[Throwable, Schema] = {

    @tailrec
    def encodeSchema[A1](name: String, schemaField: ZSchema[A1], nullable: Boolean): Field =
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
          throw EncoderError(s"Unsupported ZIO Schema type $other")
      }

    try {
      val fields = schema match {
        case record: ZSchema.Record[A] =>
          record.fields.map { case ZSchema.Field(name, schemaField, _, _, _, _) =>
            encodeSchema(name, schemaField, nullable = false)
          }
        case _                         =>
          throw EncoderError(s"Given ZIO schema mut be of type Schema.Record[Val]")
      }

      Right(new Schema(fields.toList.asJava))
    } catch {
      case encodeError: EncoderError => Left(encodeError)
      case NonFatal(ex)              => Left(EncoderError("Error encoding schema", Some(ex)))
    }
  }

  private def encodePrimitive[A](name: String, standardType: StandardType[A], nullable: Boolean): Field = {
    def namedField(arrowType: ArrowType) =
      field(name, arrowType, nullable)

    standardType match {
      case StandardType.StringType         =>
        namedField(new ArrowType.Utf8)
      case StandardType.BoolType           =>
        namedField(new ArrowType.Bool)
      case StandardType.ByteType           =>
        namedField(new ArrowType.Int(8, false))
      case StandardType.ShortType          =>
        namedField(new ArrowType.Int(16, true))
      case StandardType.IntType            =>
        namedField(new ArrowType.Int(32, true))
      case StandardType.LongType           =>
        namedField(new ArrowType.Int(64, true))
      case StandardType.FloatType          =>
        namedField(new ArrowType.FloatingPoint(FloatingPointPrecision.HALF))
      case StandardType.DoubleType         =>
        namedField(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))
      case StandardType.BinaryType         =>
        namedField(new ArrowType.Binary)
      case StandardType.CharType           =>
        namedField(new ArrowType.Int(16, false))
      case StandardType.UUIDType           =>
        namedField(new ArrowType.FixedSizeBinary(8))
      case StandardType.BigDecimalType     =>
        namedField(new ArrowType.Decimal(11, 2, 128))
      case StandardType.BigIntegerType     =>
        namedField(new ArrowType.FixedSizeBinary(8))
      case StandardType.DayOfWeekType      =>
        namedField(new ArrowType.Int(3, false))
      case StandardType.MonthType          =>
        namedField(new ArrowType.Int(4, false))
      case StandardType.MonthDayType       =>
        namedField(new ArrowType.Int(64, false))
      case StandardType.PeriodType         =>
        namedField(new ArrowType.FixedSizeBinary(8))
      case StandardType.YearType           =>
        namedField(new ArrowType.Int(16, false))
      case StandardType.YearMonthType      =>
        namedField(new ArrowType.Int(64, false))
      case StandardType.ZoneIdType         =>
        namedField(new ArrowType.Utf8)
      case StandardType.ZoneOffsetType     =>
        namedField(new ArrowType.Utf8)
      case StandardType.DurationType       =>
        namedField(new ArrowType.Int(64, false))
      case StandardType.InstantType        =>
        namedField(new ArrowType.Int(64, false))
      case StandardType.LocalDateType      =>
        namedField(new ArrowType.Utf8)
      case StandardType.LocalTimeType      =>
        namedField(new ArrowType.Utf8)
      case StandardType.LocalDateTimeType  =>
        namedField(new ArrowType.Utf8)
      case StandardType.OffsetTimeType     =>
        namedField(new ArrowType.Utf8)
      case StandardType.OffsetDateTimeType =>
        namedField(new ArrowType.Utf8)
      case StandardType.ZonedDateTimeType  =>
        namedField(new ArrowType.Utf8)
      case other                           =>
        throw EncoderError(s"Unsupported ZIO Schema StandardType $other")
    }
  }

  private[codec] def field(name: String, arrowType: ArrowType, nullable: Boolean): Field =
    new Field(name, new FieldType(nullable, arrowType, null), null)

  private[codec] def fieldNullable(name: String, arrowType: ArrowType): Field =
    field(name, arrowType, nullable = true)

  private[codec] def fieldNotNullable(name: String, arrowType: ArrowType): Field =
    field(name, arrowType, nullable = false)

}
