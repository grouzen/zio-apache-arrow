package me.mnedokushev.zio.apache.arrow.core.codec

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import zio._
import zio.schema.Derive

import scala.util.control.NonFatal

trait ValueVectorEncoder[V <: ValueVector, -A] extends ValueEncoder[A] { self =>

  final def encodeZIO(chunk: Chunk[A]): RIO[Scope with BufferAllocator, V] =
    ZIO.fromAutoCloseable(
      ZIO.serviceWithZIO[BufferAllocator] { implicit alloc =>
        ZIO.fromEither(encode(chunk))
      }
    )

  final def encode(chunk: Chunk[A])(implicit alloc: BufferAllocator): Either[Throwable, V] =
    try
      Right(encodeUnsafe(chunk, allocateVector))
    catch {
      case encoderError: EncoderError => Left(encoderError)
      case NonFatal(ex)               => Left(EncoderError("Error encoding vector", Some(ex)))

    }

  protected def encodeUnsafe(chunk: Chunk[A], vector: V)(implicit alloc: BufferAllocator): V

  def allocateVector(implicit alloc: BufferAllocator): V

  // final def contramap[B](f: B => A): ValueVectorEncoder[B, V] =
  //   new ValueVectorEncoder[B, V] {
  //     override protected def encodeUnsafe(chunk: Chunk[B])(implicit alloc: BufferAllocator): V =
  //       self.encodeUnsafe(chunk.map(f))

  //     override def encodeValue(
  //       value: B,
  //       name: Option[String],
  //       writer: FieldWriter
  //     )(implicit alloc: BufferAllocator): Unit =
  //       self.encodeValue(f(value), name, writer)

  //   }

}

object ValueVectorEncoder {

  implicit val stringEncoder: ValueVectorEncoder[VarCharVector, String]                           =
    Derive.derive[ValueVectorEncoder[VarCharVector, *], String](ValueVectorEncoderDeriver.default[VarCharVector])
  implicit val boolEncoder: ValueVectorEncoder[BitVector, Boolean]                                =
    Derive.derive[ValueVectorEncoder[BitVector, *], Boolean](ValueVectorEncoderDeriver.default[BitVector])
  implicit val byteEncoder: ValueVectorEncoder[UInt1Vector, Byte]                                 =
    Derive.derive[ValueVectorEncoder[UInt1Vector, *], Byte](ValueVectorEncoderDeriver.default[UInt1Vector])
  implicit val shortEncoder: ValueVectorEncoder[SmallIntVector, Short]                            =
    Derive.derive[ValueVectorEncoder[SmallIntVector, *], Short](ValueVectorEncoderDeriver.default[SmallIntVector])
  implicit val intEncoder: ValueVectorEncoder[IntVector, Int]                                     =
    Derive.derive[ValueVectorEncoder[IntVector, *], Int](ValueVectorEncoderDeriver.default[IntVector])
  implicit val longEncoder: ValueVectorEncoder[BigIntVector, Long]                                =
    Derive.derive[ValueVectorEncoder[BigIntVector, *], Long](ValueVectorEncoderDeriver.default[BigIntVector])
  implicit val floatEncoder: ValueVectorEncoder[Float4Vector, Float]                              =
    Derive.derive[ValueVectorEncoder[Float4Vector, *], Float](ValueVectorEncoderDeriver.default[Float4Vector])
  implicit val doubleEncoder: ValueVectorEncoder[Float8Vector, Double]                            =
    Derive.derive[ValueVectorEncoder[Float8Vector, *], Double](ValueVectorEncoderDeriver.default[Float8Vector])
  implicit val binaryEncoder: ValueVectorEncoder[LargeVarBinaryVector, Chunk[Byte]]               =
    Derive.derive[ValueVectorEncoder[LargeVarBinaryVector, *], Chunk[Byte]](
      ValueVectorEncoderDeriver.default[LargeVarBinaryVector]
    )
  implicit val charEncoder: ValueVectorEncoder[UInt2Vector, Char]                                 =
    Derive.derive[ValueVectorEncoder[UInt2Vector, *], Char](ValueVectorEncoderDeriver.default[UInt2Vector])
  implicit val uuidEncoder: ValueVectorEncoder[VarBinaryVector, java.util.UUID]                   =
    Derive.derive[ValueVectorEncoder[VarBinaryVector, *], java.util.UUID](
      ValueVectorEncoderDeriver.default[VarBinaryVector]
    )
  implicit val bigDecimalEncoder: ValueVectorEncoder[DecimalVector, java.math.BigDecimal]         =
    Derive.derive[ValueVectorEncoder[DecimalVector, *], java.math.BigDecimal](
      ValueVectorEncoderDeriver.default[DecimalVector]
    )
  implicit val bigIntegerEncoder: ValueVectorEncoder[VarBinaryVector, java.math.BigInteger]       =
    Derive.derive[ValueVectorEncoder[VarBinaryVector, *], java.math.BigInteger](
      ValueVectorEncoderDeriver.default[VarBinaryVector]
    )
  implicit val dayOfWeekEncoder: ValueVectorEncoder[IntVector, java.time.DayOfWeek]               =
    Derive.derive[ValueVectorEncoder[IntVector, *], java.time.DayOfWeek](ValueVectorEncoderDeriver.default[IntVector])
  implicit val monthDecoder: ValueVectorEncoder[IntVector, java.time.Month]                       =
    Derive.derive[ValueVectorEncoder[IntVector, *], java.time.Month](ValueVectorEncoderDeriver.default[IntVector])
  implicit val monthDayDecoder: ValueVectorEncoder[BigIntVector, java.time.MonthDay]              =
    Derive.derive[ValueVectorEncoder[BigIntVector, *], java.time.MonthDay](
      ValueVectorEncoderDeriver.default[BigIntVector]
    )
  implicit val periodDecoder: ValueVectorEncoder[VarBinaryVector, java.time.Period]               =
    Derive.derive[ValueVectorEncoder[VarBinaryVector, *], java.time.Period](
      ValueVectorEncoderDeriver.default[VarBinaryVector]
    )
  implicit val yearDecoder: ValueVectorEncoder[IntVector, java.time.Year]                         =
    Derive.derive[ValueVectorEncoder[IntVector, *], java.time.Year](ValueVectorEncoderDeriver.default[IntVector])
  implicit val yearMonthDecoder: ValueVectorEncoder[BigIntVector, java.time.YearMonth]            =
    Derive.derive[ValueVectorEncoder[BigIntVector, *], java.time.YearMonth](
      ValueVectorEncoderDeriver.default[BigIntVector]
    )
  implicit val zoneIdDecoder: ValueVectorEncoder[VarCharVector, java.time.ZoneId]                 =
    Derive.derive[ValueVectorEncoder[VarCharVector, *], java.time.ZoneId](
      ValueVectorEncoderDeriver.default[VarCharVector]
    )
  implicit val zoneOffsetDecoder: ValueVectorEncoder[VarCharVector, java.time.ZoneOffset]         =
    Derive.derive[ValueVectorEncoder[VarCharVector, *], java.time.ZoneOffset](
      ValueVectorEncoderDeriver.default[VarCharVector]
    )
  implicit val durationDecoder: ValueVectorEncoder[BigIntVector, java.time.Duration]            =
    Derive.derive[ValueVectorEncoder[BigIntVector, *], java.time.Duration](
      ValueVectorEncoderDeriver.default[BigIntVector]
    )
  implicit val instantDecoder: ValueVectorEncoder[BigIntVector, java.time.Instant]                =
    Derive.derive[ValueVectorEncoder[BigIntVector, *], java.time.Instant](
      ValueVectorEncoderDeriver.default[BigIntVector]
    )
  implicit val localDateDecoder: ValueVectorEncoder[VarCharVector, java.time.LocalDate]           =
    Derive.derive[ValueVectorEncoder[VarCharVector, *], java.time.LocalDate](
      ValueVectorEncoderDeriver.default[VarCharVector]
    )
  implicit val localTimeDecoder: ValueVectorEncoder[VarCharVector, java.time.LocalTime]           =
    Derive.derive[ValueVectorEncoder[VarCharVector, *], java.time.LocalTime](
      ValueVectorEncoderDeriver.default[VarCharVector]
    )
  implicit val localDateTimeDecoder: ValueVectorEncoder[VarCharVector, java.time.LocalDateTime]   =
    Derive.derive[ValueVectorEncoder[VarCharVector, *], java.time.LocalDateTime](
      ValueVectorEncoderDeriver.default[VarCharVector]
    )
  implicit val offsetTimeDecoder: ValueVectorEncoder[VarCharVector, java.time.OffsetTime]         =
    Derive.derive[ValueVectorEncoder[VarCharVector, *], java.time.OffsetTime](
      ValueVectorEncoderDeriver.default[VarCharVector]
    )
  implicit val offsetDateTimeDecoder: ValueVectorEncoder[VarCharVector, java.time.OffsetDateTime] =
    Derive.derive[ValueVectorEncoder[VarCharVector, *], java.time.OffsetDateTime](
      ValueVectorEncoderDeriver.default[VarCharVector]
    )
  implicit val zonedDateTimeDecoder: ValueVectorEncoder[VarCharVector, java.time.ZonedDateTime]   =
    Derive.derive[ValueVectorEncoder[VarCharVector, *], java.time.ZonedDateTime](
      ValueVectorEncoderDeriver.default[VarCharVector]
    )

}
