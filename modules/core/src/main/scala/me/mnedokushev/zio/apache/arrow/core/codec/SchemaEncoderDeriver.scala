package me.mnedokushev.zio.apache.arrow.core.codec

import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.{ ArrowType, Field, Schema => JSchema }
import zio.Chunk
import zio.schema.{ Deriver, Schema, StandardType }

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

object SchemaEncoderDeriver {

  val default: Deriver[SchemaEncoder] = new Deriver[SchemaEncoder] {

    override def deriveRecord[A](
      record: Schema.Record[A],
      fields: => Chunk[Deriver.WrappedF[SchemaEncoder, _]],
      summoned: => Option[SchemaEncoder[A]]
    ): SchemaEncoder[A] = new SchemaEncoder[A] {

      override def encode(implicit schema: Schema[A]): Either[Throwable, JSchema] =
        try {
          val fields0 =
            record.fields.zip(fields.map(_.unwrap)).map { case (field, encoder) =>
              encoder.encodeField(field.name, nullable = false)
            }

          Right(new JSchema(fields0.toList.asJava))
        } catch {
          case encodeError: EncoderError => Left(encodeError)
          case NonFatal(ex)              => Left(EncoderError("Error encoding schema", Some(ex)))
        }

      override def encodeField(name: String, nullable: Boolean): Field =
        SchemaEncoder.field(name, new ArrowType.Struct, nullable)

    }

    override def deriveEnum[A](
      `enum`: Schema.Enum[A],
      cases: => Chunk[Deriver.WrappedF[SchemaEncoder, _]],
      summoned: => Option[SchemaEncoder[A]]
    ): SchemaEncoder[A] =
      new SchemaEncoder[A] {

        override def encodeField(name: String, nullable: Boolean): Field =
          throw EncoderError(s"Unsupported ZIO Schema type ${`enum`}")

      }

    override def derivePrimitive[A](
      st: StandardType[A],
      summoned: => Option[SchemaEncoder[A]]
    ): SchemaEncoder[A] = new SchemaEncoder[A] {

      override def encodeField(name: String, nullable: Boolean): Field = {
        def namedField(arrowType: ArrowType) =
          SchemaEncoder.field(name, arrowType, nullable)

        st match {
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

    }

    override def deriveOption[A](
      option: Schema.Optional[A],
      inner: => SchemaEncoder[A],
      summoned: => Option[SchemaEncoder[Option[A]]]
    ): SchemaEncoder[Option[A]] = new SchemaEncoder[Option[A]] {

      override def encodeField(name: String, nullable: Boolean): Field =
        inner.encodeField(name, nullable = true)

    }

    override def deriveSequence[C[_], A](
      sequence: Schema.Sequence[C[A], A, _],
      inner: => SchemaEncoder[A],
      summoned: => Option[SchemaEncoder[C[A]]]
    ): SchemaEncoder[C[A]] = new SchemaEncoder[C[A]] {

      override def encodeField(name: String, nullable: Boolean): Field =
        SchemaEncoder.field(name, new ArrowType.List, nullable)

    }

    override def deriveMap[K, V](
      map: Schema.Map[K, V],
      key: => SchemaEncoder[K],
      value: => SchemaEncoder[V],
      summoned: => Option[SchemaEncoder[Map[K, V]]]
    ): SchemaEncoder[Map[K, V]] = new SchemaEncoder[Map[K, V]] {

      override def encodeField(name: String, nullable: Boolean): Field =
        throw EncoderError(s"Unsupported ZIO Schema type $map")

    }

    override def deriveTransformedRecord[A, B](
      record: Schema.Record[A],
      transform: Schema.Transform[A, B, _],
      fields: => Chunk[Deriver.WrappedF[SchemaEncoder, _]],
      summoned: => Option[SchemaEncoder[B]]
    ): SchemaEncoder[B] = new SchemaEncoder[B] {

      override def encodeField(name: String, nullable: Boolean): Field =
        throw EncoderError(s"Unsupported ZIO Schema type $record")

    }

  }.cached

  def summoned: Deriver[SchemaEncoder] = default.autoAcceptSummoned

}
