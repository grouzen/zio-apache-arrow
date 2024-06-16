package me.mnedokushev.zio.apache.arrow.core.codec

import org.apache.arrow.vector._
import zio._
import zio.schema.Derive

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

object ValueVectorDecoder {

  implicit val stringDecoder: ValueVectorDecoder[VarCharVector, String]                           =
    Derive.derive[ValueVectorDecoder[VarCharVector, *], String](ValueVectorDecoderDeriver.default[VarCharVector])
  implicit val boolDecoder: ValueVectorDecoder[BitVector, Boolean]                                =
    Derive.derive[ValueVectorDecoder[BitVector, *], Boolean](ValueVectorDecoderDeriver.default[BitVector])
  implicit val byteDecoder: ValueVectorDecoder[UInt1Vector, Byte]                                 =
    Derive.derive[ValueVectorDecoder[UInt1Vector, *], Byte](ValueVectorDecoderDeriver.default[UInt1Vector])
  implicit val shortDecoder: ValueVectorDecoder[SmallIntVector, Short]                            =
    Derive.derive[ValueVectorDecoder[SmallIntVector, *], Short](ValueVectorDecoderDeriver.default[SmallIntVector])
  implicit val intDecoder: ValueVectorDecoder[IntVector, Int]                                     =
    Derive.derive[ValueVectorDecoder[IntVector, *], Int](ValueVectorDecoderDeriver.default[IntVector])
  implicit val longDecoder: ValueVectorDecoder[BigIntVector, Long]                                =
    Derive.derive[ValueVectorDecoder[BigIntVector, *], Long](ValueVectorDecoderDeriver.default[BigIntVector])
  implicit val floatDecoder: ValueVectorDecoder[Float4Vector, Float]                              =
    Derive.derive[ValueVectorDecoder[Float4Vector, *], Float](ValueVectorDecoderDeriver.default[Float4Vector])
  implicit val doubleDecoder: ValueVectorDecoder[Float8Vector, Double]                            =
    Derive.derive[ValueVectorDecoder[Float8Vector, *], Double](ValueVectorDecoderDeriver.default[Float8Vector])
  implicit val binaryDecoder: ValueVectorDecoder[LargeVarBinaryVector, Chunk[Byte]]               =
    Derive.derive[ValueVectorDecoder[LargeVarBinaryVector, *], Chunk[Byte]](
      ValueVectorDecoderDeriver.default[LargeVarBinaryVector]
    )
  implicit val charDecoder: ValueVectorDecoder[UInt2Vector, Char]                                 =
    Derive.derive[ValueVectorDecoder[UInt2Vector, *], Char](ValueVectorDecoderDeriver.default[UInt2Vector])
  implicit val uuidDecoder: ValueVectorDecoder[VarBinaryVector, java.util.UUID]                   =
    Derive.derive[ValueVectorDecoder[VarBinaryVector, *], java.util.UUID](
      ValueVectorDecoderDeriver.default[VarBinaryVector]
    )
  implicit val bigDecimalDecoder: ValueVectorDecoder[DecimalVector, java.math.BigDecimal]         =
    Derive.derive[ValueVectorDecoder[DecimalVector, *], java.math.BigDecimal](
      ValueVectorDecoderDeriver.default[DecimalVector]
    )
  implicit val bigIntegerDecoder: ValueVectorDecoder[VarBinaryVector, java.math.BigInteger]       =
    Derive.derive[ValueVectorDecoder[VarBinaryVector, *], java.math.BigInteger](
      ValueVectorDecoderDeriver.default[VarBinaryVector]
    )
  implicit val dayOfWeekDecoder: ValueVectorDecoder[IntVector, java.time.DayOfWeek]               =
    Derive.derive[ValueVectorDecoder[IntVector, *], java.time.DayOfWeek](
      ValueVectorDecoderDeriver.default[IntVector]
    )
  implicit val monthDecoder: ValueVectorDecoder[IntVector, java.time.Month]                       =
    Derive.derive[ValueVectorDecoder[IntVector, *], java.time.Month](ValueVectorDecoderDeriver.default[IntVector])
  implicit val monthDayDecoder: ValueVectorDecoder[BigIntVector, java.time.MonthDay]              =
    Derive.derive[ValueVectorDecoder[BigIntVector, *], java.time.MonthDay](
      ValueVectorDecoderDeriver.default[BigIntVector]
    )
  implicit val periodDecoder: ValueVectorDecoder[VarBinaryVector, java.time.Period]               =
    Derive.derive[ValueVectorDecoder[VarBinaryVector, *], java.time.Period](
      ValueVectorDecoderDeriver.default[VarBinaryVector]
    )
  implicit val yearDecoder: ValueVectorDecoder[IntVector, java.time.Year]                         =
    Derive.derive[ValueVectorDecoder[IntVector, *], java.time.Year](ValueVectorDecoderDeriver.default[IntVector])
  implicit val yearMonthDecoder: ValueVectorDecoder[BigIntVector, java.time.YearMonth]            =
    Derive.derive[ValueVectorDecoder[BigIntVector, *], java.time.YearMonth](
      ValueVectorDecoderDeriver.default[BigIntVector]
    )
  implicit val zoneIdDecoder: ValueVectorDecoder[VarCharVector, java.time.ZoneId]                 =
    Derive.derive[ValueVectorDecoder[VarCharVector, *], java.time.ZoneId](
      ValueVectorDecoderDeriver.default[VarCharVector]
    )
  implicit val zoneOffsetDecoder: ValueVectorDecoder[VarCharVector, java.time.ZoneOffset]         =
    Derive.derive[ValueVectorDecoder[VarCharVector, *], java.time.ZoneOffset](
      ValueVectorDecoderDeriver.default[VarCharVector]
    )
  implicit val durationDecoder: ValueVectorDecoder[BigIntVector, java.time.Duration]            =
    Derive.derive[ValueVectorDecoder[BigIntVector, *], java.time.Duration](
      ValueVectorDecoderDeriver.default[BigIntVector]
    )
  implicit val instantDecoder: ValueVectorDecoder[BigIntVector, java.time.Instant]                =
    Derive.derive[ValueVectorDecoder[BigIntVector, *], java.time.Instant](
      ValueVectorDecoderDeriver.default[BigIntVector]
    )
  implicit val localDateDecoder: ValueVectorDecoder[VarCharVector, java.time.LocalDate]           =
    Derive.derive[ValueVectorDecoder[VarCharVector, *], java.time.LocalDate](
      ValueVectorDecoderDeriver.default[VarCharVector]
    )
  implicit val localTimeDecoder: ValueVectorDecoder[VarCharVector, java.time.LocalTime]           =
    Derive.derive[ValueVectorDecoder[VarCharVector, *], java.time.LocalTime](
      ValueVectorDecoderDeriver.default[VarCharVector]
    )
  implicit val localDateTimeDecoder: ValueVectorDecoder[VarCharVector, java.time.LocalDateTime]   =
    Derive.derive[ValueVectorDecoder[VarCharVector, *], java.time.LocalDateTime](
      ValueVectorDecoderDeriver.default[VarCharVector]
    )
  implicit val offsetTimeDecoder: ValueVectorDecoder[VarCharVector, java.time.OffsetTime]         =
    Derive.derive[ValueVectorDecoder[VarCharVector, *], java.time.OffsetTime](
      ValueVectorDecoderDeriver.default[VarCharVector]
    )
  implicit val offsetDateTimeDecoder: ValueVectorDecoder[VarCharVector, java.time.OffsetDateTime] =
    Derive.derive[ValueVectorDecoder[VarCharVector, *], java.time.OffsetDateTime](
      ValueVectorDecoderDeriver.default[VarCharVector]
    )
  implicit val zonedDateTimeDecoder: ValueVectorDecoder[VarCharVector, java.time.ZonedDateTime]   =
    Derive.derive[ValueVectorDecoder[VarCharVector, *], java.time.ZonedDateTime](
      ValueVectorDecoderDeriver.default[VarCharVector]
    )

}
