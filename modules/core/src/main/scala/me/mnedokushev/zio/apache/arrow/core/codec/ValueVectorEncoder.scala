package me.mnedokushev.zio.apache.arrow.core.codec

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.writer.FieldWriter
import org.apache.arrow.vector.complex.{ ListVector, StructVector }
import zio._
import zio.schema.{ Derive, Deriver, Factory, Schema, StandardType }

import scala.annotation.nowarn
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

}

object ValueVectorEncoder {

  def primitive[V <: ValueVector, A](
    allocateVec: BufferAllocator => V,
    getWriter: V => FieldWriter,
    encodeTopLevel: (A, FieldWriter, BufferAllocator) => Unit,
    encodeNested: (A, Option[String], FieldWriter, BufferAllocator) => Unit
  )(implicit @nowarn ev: StandardType[A]): ValueVectorEncoder[V, A] =
    new ValueVectorEncoder[V, A] {

      override def encodeUnsafe(chunk: Chunk[Option[A]], nullable: Boolean)(implicit alloc: BufferAllocator): V = {
        val vec                 = allocateVec(alloc)
        val writer: FieldWriter = getWriter(vec)
        val len                 = chunk.length
        val it                  = chunk.iterator.zipWithIndex

        it.foreach { case (v, i) =>
          writer.setPosition(i)

          if (nullable && v.isEmpty)
            writer.writeNull()
          else
            encodeTopLevel(v.get, writer, alloc)
        }

        vec.setValueCount(len)
        vec.asInstanceOf[V]
      }

      override def encodeValue(value: A, name: Option[String], writer: FieldWriter)(implicit
        alloc: BufferAllocator
      ): Unit =
        encodeNested(value, name, writer, alloc)

    }

  implicit def encoder[V <: ValueVector, A: Schema](implicit factory: Factory[A]): ValueVectorEncoder[V, A] =
    factory.derive(ValueVectorEncoderDeriver.default[V])

  implicit val stringEncoder: ValueVectorEncoder[VarCharVector, String]                           =
    encoder[VarCharVector, String]
  implicit val boolEncoder: ValueVectorEncoder[BitVector, Boolean]                                =
    encoder[BitVector, Boolean]
  implicit val byteEncoder: ValueVectorEncoder[UInt1Vector, Byte]                                 =
    encoder[UInt1Vector, Byte]
  implicit val shortEncoder: ValueVectorEncoder[SmallIntVector, Short]                            =
    encoder[SmallIntVector, Short]
  implicit val intEncoder: ValueVectorEncoder[IntVector, Int]                                     =
    encoder[IntVector, Int]
  implicit val longEncoder: ValueVectorEncoder[BigIntVector, Long]                                =
    encoder[BigIntVector, Long]
  implicit val floatEncoder: ValueVectorEncoder[Float4Vector, Float]                              =
    encoder[Float4Vector, Float]
  implicit val doubleEncoder: ValueVectorEncoder[Float8Vector, Double]                            =
    encoder[Float8Vector, Double]
  implicit val binaryEncoder: ValueVectorEncoder[LargeVarBinaryVector, Chunk[Byte]]               =
    encoder[LargeVarBinaryVector, Chunk[Byte]]
  implicit val charEncoder: ValueVectorEncoder[UInt2Vector, Char]                                 =
    encoder[UInt2Vector, Char]
  implicit val uuidEncoder: ValueVectorEncoder[VarBinaryVector, java.util.UUID]                   =
    encoder[VarBinaryVector, java.util.UUID]
  implicit val bigDecimalEncoder: ValueVectorEncoder[DecimalVector, java.math.BigDecimal]         =
    encoder[DecimalVector, java.math.BigDecimal]
  implicit val bigIntegerEncoder: ValueVectorEncoder[VarBinaryVector, java.math.BigInteger]       =
    encoder[VarBinaryVector, java.math.BigInteger]
  implicit val dayOfWeekEncoder: ValueVectorEncoder[IntVector, java.time.DayOfWeek]               =
    encoder[IntVector, java.time.DayOfWeek]
  implicit val monthEncoder: ValueVectorEncoder[IntVector, java.time.Month]                       =
    encoder[IntVector, java.time.Month]
  implicit val monthDayEncoder: ValueVectorEncoder[BigIntVector, java.time.MonthDay]              =
    encoder[BigIntVector, java.time.MonthDay]
  implicit val periodEncoder: ValueVectorEncoder[VarBinaryVector, java.time.Period]               =
    encoder[VarBinaryVector, java.time.Period]
  implicit val yearEncoder: ValueVectorEncoder[IntVector, java.time.Year]                         =
    encoder[IntVector, java.time.Year]
  implicit val yearMonthEncoder: ValueVectorEncoder[BigIntVector, java.time.YearMonth]            =
    encoder[BigIntVector, java.time.YearMonth]
  implicit val zoneIdEncoder: ValueVectorEncoder[VarCharVector, java.time.ZoneId]                 =
    encoder[VarCharVector, java.time.ZoneId]
  implicit val zoneOffsetEncoder: ValueVectorEncoder[VarCharVector, java.time.ZoneOffset]         =
    encoder[VarCharVector, java.time.ZoneOffset]
  implicit val durationEncoder: ValueVectorEncoder[BigIntVector, java.time.Duration]              =
    encoder[BigIntVector, java.time.Duration]
  implicit val instantEncoder: ValueVectorEncoder[BigIntVector, java.time.Instant]                =
    encoder[BigIntVector, java.time.Instant]
  implicit val localDateEncoder: ValueVectorEncoder[VarCharVector, java.time.LocalDate]           =
    encoder[VarCharVector, java.time.LocalDate]
  implicit val localTimeEncoder: ValueVectorEncoder[VarCharVector, java.time.LocalTime]           =
    encoder[VarCharVector, java.time.LocalTime]
  implicit val localDateTimeEncoder: ValueVectorEncoder[VarCharVector, java.time.LocalDateTime]   =
    encoder[VarCharVector, java.time.LocalDateTime]
  implicit val offsetTimeEncoder: ValueVectorEncoder[VarCharVector, java.time.OffsetTime]         =
    encoder[VarCharVector, java.time.OffsetTime]
  implicit val offsetDateTimeEncoder: ValueVectorEncoder[VarCharVector, java.time.OffsetDateTime] =
    encoder[VarCharVector, java.time.OffsetDateTime]
  implicit val zonedDateTimeEncoder: ValueVectorEncoder[VarCharVector, java.time.ZonedDateTime]   =
    encoder[VarCharVector, java.time.ZonedDateTime]

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

  implicit def structEncoder[A](implicit
    factory: Factory[A],
    schema: Schema[A]
  ): ValueVectorEncoder[StructVector, A] =
    structEncoderFromDefaultDeriver[A]

  def structEncoderFromDeriver[A](
    deriver: Deriver[ValueVectorEncoder[StructVector, *]]
  )(implicit factory: Factory[A], schema: Schema[A]): ValueVectorEncoder[StructVector, A] =
    factory.derive[ValueVectorEncoder[StructVector, *]](deriver)

  def structEncoderFromDefaultDeriver[A](implicit
    factory: Factory[A],
    schema: Schema[A]
  ): ValueVectorEncoder[StructVector, A] =
    structEncoderFromDeriver[A](ValueVectorEncoderDeriver.default[StructVector])

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
