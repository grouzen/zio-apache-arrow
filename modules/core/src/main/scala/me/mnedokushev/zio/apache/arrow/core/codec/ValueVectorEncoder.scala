package me.mnedokushev.zio.apache.arrow.core.codec

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import zio._
import zio.schema.Derive

import scala.util.control.NonFatal
import zio.schema.Factory
import zio.schema.Schema
import zio.schema.Deriver
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.complex.writer.FieldWriter

trait ValueVectorEncoder[V <: ValueVector, -A] extends ValueEncoder[A] { self =>

  final def encodeZIO(chunk: Chunk[A]): RIO[Scope with BufferAllocator, V] =
    ZIO.fromAutoCloseable(
      ZIO.serviceWithZIO[BufferAllocator] { implicit alloc =>
        ZIO.fromEither(encode(chunk))
      }
    )

  final def encode(chunk: Chunk[A])(implicit alloc: BufferAllocator): Either[Throwable, V] =
    try
      Right(encodeUnsafe(chunk.map(Some(_)), nullable = false))
    catch {
      case encoderError: EncoderError => Left(encoderError)
      case NonFatal(ex)               => Left(EncoderError("Error encoding vector", Some(ex)))

    }

  def encodeUnsafe(chunk: Chunk[Option[A]], nullable: Boolean)(implicit alloc: BufferAllocator): V

  final def contramap[B](f: B => A): ValueVectorEncoder[V, B] =
    new ValueVectorEncoder[V, B] {

      override def encodeUnsafe(chunk: Chunk[Option[B]], nullable: Boolean)(implicit alloc: BufferAllocator): V =
        self.encodeUnsafe(chunk.map(_.map(f)), nullable)

      override def encodeValue(value: B, name: Option[String], writer: FieldWriter)(implicit
        alloc: BufferAllocator
      ): Unit =
        self.encodeValue(f(value), name, writer)

    }

  // def allocateVector(implicit alloc: BufferAllocator): V

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
  implicit val monthEncoder: ValueVectorEncoder[IntVector, java.time.Month]                       =
    Derive.derive[ValueVectorEncoder[IntVector, *], java.time.Month](ValueVectorEncoderDeriver.default[IntVector])
  implicit val monthDayEncoder: ValueVectorEncoder[BigIntVector, java.time.MonthDay]              =
    Derive.derive[ValueVectorEncoder[BigIntVector, *], java.time.MonthDay](
      ValueVectorEncoderDeriver.default[BigIntVector]
    )
  implicit val periodEncoder: ValueVectorEncoder[VarBinaryVector, java.time.Period]               =
    Derive.derive[ValueVectorEncoder[VarBinaryVector, *], java.time.Period](
      ValueVectorEncoderDeriver.default[VarBinaryVector]
    )
  implicit val yearEncoder: ValueVectorEncoder[IntVector, java.time.Year]                         =
    Derive.derive[ValueVectorEncoder[IntVector, *], java.time.Year](ValueVectorEncoderDeriver.default[IntVector])
  implicit val yearMonthEncoder: ValueVectorEncoder[BigIntVector, java.time.YearMonth]            =
    Derive.derive[ValueVectorEncoder[BigIntVector, *], java.time.YearMonth](
      ValueVectorEncoderDeriver.default[BigIntVector]
    )
  implicit val zoneIdEncoder: ValueVectorEncoder[VarCharVector, java.time.ZoneId]                 =
    Derive.derive[ValueVectorEncoder[VarCharVector, *], java.time.ZoneId](
      ValueVectorEncoderDeriver.default[VarCharVector]
    )
  implicit val zoneOffsetEncoder: ValueVectorEncoder[VarCharVector, java.time.ZoneOffset]         =
    Derive.derive[ValueVectorEncoder[VarCharVector, *], java.time.ZoneOffset](
      ValueVectorEncoderDeriver.default[VarCharVector]
    )
  implicit val durationEncoder: ValueVectorEncoder[BigIntVector, java.time.Duration]              =
    Derive.derive[ValueVectorEncoder[BigIntVector, *], java.time.Duration](
      ValueVectorEncoderDeriver.default[BigIntVector]
    )
  implicit val instantEncoder: ValueVectorEncoder[BigIntVector, java.time.Instant]                =
    Derive.derive[ValueVectorEncoder[BigIntVector, *], java.time.Instant](
      ValueVectorEncoderDeriver.default[BigIntVector]
    )
  implicit val localDateEncoder: ValueVectorEncoder[VarCharVector, java.time.LocalDate]           =
    Derive.derive[ValueVectorEncoder[VarCharVector, *], java.time.LocalDate](
      ValueVectorEncoderDeriver.default[VarCharVector]
    )
  implicit val localTimeEncoder: ValueVectorEncoder[VarCharVector, java.time.LocalTime]           =
    Derive.derive[ValueVectorEncoder[VarCharVector, *], java.time.LocalTime](
      ValueVectorEncoderDeriver.default[VarCharVector]
    )
  implicit val localDateTimeEncoder: ValueVectorEncoder[VarCharVector, java.time.LocalDateTime]   =
    Derive.derive[ValueVectorEncoder[VarCharVector, *], java.time.LocalDateTime](
      ValueVectorEncoderDeriver.default[VarCharVector]
    )
  implicit val offsetTimeEncoder: ValueVectorEncoder[VarCharVector, java.time.OffsetTime]         =
    Derive.derive[ValueVectorEncoder[VarCharVector, *], java.time.OffsetTime](
      ValueVectorEncoderDeriver.default[VarCharVector]
    )
  implicit val offsetDateTimeEncoder: ValueVectorEncoder[VarCharVector, java.time.OffsetDateTime] =
    Derive.derive[ValueVectorEncoder[VarCharVector, *], java.time.OffsetDateTime](
      ValueVectorEncoderDeriver.default[VarCharVector]
    )
  implicit val zonedDateTimeEncoder: ValueVectorEncoder[VarCharVector, java.time.ZonedDateTime]   =
    Derive.derive[ValueVectorEncoder[VarCharVector, *], java.time.ZonedDateTime](
      ValueVectorEncoderDeriver.default[VarCharVector]
    )

  implicit def listEncoder[A, C[_]](implicit
    factory: Factory[C[A]],
    schema: Schema[C[A]]
  ): ValueVectorEncoder[ListVector, C[A]] =
    listEncoderFromDefaultDeriver[A, C]

  implicit def listChunkEncoder[A](implicit
    factory: Factory[Chunk[A]],
    schema: Schema[Chunk[A]]
  ): ValueVectorEncoder[ListVector, Chunk[A]] =
    listEncoder[A, Chunk]

  implicit def listOptionEncoder[A, C[_]](implicit
    factory: Factory[C[Option[A]]],
    schema: Schema[C[Option[A]]]
  ): ValueVectorEncoder[ListVector, C[Option[A]]] =
    listEncoder[Option[A], C]

  implicit def listChunkOptionEncoder[A](implicit
    factory: Factory[Chunk[Option[A]]],
    schema: Schema[Chunk[Option[A]]]
  ): ValueVectorEncoder[ListVector, Chunk[Option[A]]] =
    listChunkEncoder[Option[A]]

  def listEncoderFromDeriver[A, C[_]](
    deriver: Deriver[ValueVectorEncoder[ListVector, *]]
  )(implicit factory: Factory[C[A]], schema: Schema[C[A]]): ValueVectorEncoder[ListVector, C[A]] =
    factory.derive[ValueVectorEncoder[ListVector, *]](deriver)

  def listEncoderFromDefaultDeriver[A, C[_]](implicit
    factory: Factory[C[A]],
    schema: Schema[C[A]]
  ): ValueVectorEncoder[ListVector, C[A]] =
    listEncoderFromDeriver[A, C](ValueVectorEncoderDeriver.default[ListVector])

  def listEncoderFromSummonedDeriver[A, C[_]](implicit
    factory: Factory[C[A]],
    schema: Schema[C[A]]
  ): ValueVectorEncoder[ListVector, C[A]] =
    listEncoderFromDeriver[A, C](ValueVectorEncoderDeriver.summoned[ListVector])

  implicit def optionEncoder[V <: ValueVector, A](implicit
    factory: Factory[Option[A]],
    schema: Schema[Option[A]]
  ): ValueVectorEncoder[V, Option[A]] =
    optionEncoderFromDefaultDeriver[V, A]

  implicit def optionListEncoder[A, C[_]](implicit
    factory: Factory[Option[C[A]]],
    schema: Schema[Option[C[A]]]
  ): ValueVectorEncoder[ListVector, Option[C[A]]] =
    optionEncoder[ListVector, C[A]]

  implicit def optionListChunkEncoder[A](implicit
    factory: Factory[Option[Chunk[A]]],
    schema: Schema[Option[Chunk[A]]]
  ): ValueVectorEncoder[ListVector, Option[Chunk[A]]] =
    optionEncoder[ListVector, Chunk[A]]

  def optionEncoderFromDeriver[V <: ValueVector, A](deriver: Deriver[ValueVectorEncoder[V, *]])(implicit
    factory: Factory[Option[A]],
    schema: Schema[Option[A]]
  ): ValueVectorEncoder[V, Option[A]] =
    factory.derive[ValueVectorEncoder[V, *]](deriver)

  def optionEncoderFromDefaultDeriver[V <: ValueVector, A](implicit
    factory: Factory[Option[A]],
    schema: Schema[Option[A]]
  ): ValueVectorEncoder[V, Option[A]] =
    optionEncoderFromDeriver(ValueVectorEncoderDeriver.default[V])

}
