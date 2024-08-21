package me.mnedokushev.zio.apache.arrow.core.codec

import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.reader.FieldReader
import org.apache.arrow.vector.complex.{ ListVector, StructVector }
import zio._
import zio.schema.{ Derive, Deriver, DynamicValue, Factory, Schema, StandardType }

import scala.util.control.NonFatal

trait ValueVectorDecoder[V <: ValueVector, A] extends ValueDecoder[A] { self =>

  final def decodeZIO(vec: V): Task[Chunk[A]] =
    ZIO.fromEither(decode(vec))

  final def decode(vec: V): Either[Throwable, Chunk[A]] =
    try
      Right(decodeUnsafe(vec))
    catch {
      case decoderError: DecoderError => Left(decoderError)
      case NonFatal(ex)               => Left(DecoderError("Error decoding vector", Some(ex)))
    }

  def decodeUnsafe(vec: V): Chunk[A]

  def decodeNullableUnsafe(vec: V): Chunk[Option[A]]

  final def map[B](f: A => B)(implicit schemaSrc: Schema[A], schemaDst: Schema[B]): ValueVectorDecoder[V, B] =
    new ValueVectorDecoder[V, B] {

      override def decodeUnsafe(vec: V): Chunk[B] =
        self.decodeUnsafe(vec).map(f)

      override def decodeNullableUnsafe(vec: V): Chunk[Option[B]] =
        self.decodeNullableUnsafe(vec).map(_.map(f))

      override def decodeValue[V0 <: ValueVector](
        name: Option[String],
        reader: FieldReader,
        vec: V0,
        idx: Int
      ): DynamicValue =
        self
          .decodeValue(name, reader, vec, idx)
          .toValue(schemaSrc)
          .map(a => schemaDst.toDynamic(f(a)))
          .toTry
          .get

    }

}

object ValueVectorDecoder {

  def primitive[V <: ValueVector, A](
    decode0: (StandardType[A], FieldReader) => DynamicValue
  )(implicit st: StandardType[A]): ValueVectorDecoder[V, A] =
    new ValueVectorDecoder[V, A] {

      override def decodeUnsafe(vec: V): Chunk[A] = {
        var idx     = 0
        val len     = vec.getValueCount
        val builder = ChunkBuilder.make[A](len)
        val reader  = vec.getReader

        while (idx < len) {
          reader.setPosition(idx)
          val dynamicValue = decode0(st, reader)

          dynamicValue.toTypedValue(Schema.primitive(st)) match {
            case Right(v)      =>
              builder.addOne(v)
              idx += 1
            case Left(message) =>
              throw DecoderError(message)
          }
        }

        builder.result()
      }

      override def decodeNullableUnsafe(vec: V): Chunk[Option[A]] = {
        var idx     = 0
        val len     = vec.getValueCount
        val builder = ChunkBuilder.make[Option[A]](len)
        val reader  = vec.getReader

        while (idx < len) {
          if (!vec.isNull(idx)) {
            reader.setPosition(idx)
            val dynamicValue = decode0(st, reader)

            dynamicValue.toTypedValue(Schema.primitive(st)) match {
              case Right(v)      =>
                builder.addOne(Some(v))
              case Left(message) =>
                throw DecoderError(message)
            }
          } else {
            builder.addOne(None)
          }

          idx += 1
        }

        builder.result()
      }

      override def decodeValue[V0 <: ValueVector](
        name: Option[String],
        reader: FieldReader,
        vec: V0,
        idx: Int
      ): DynamicValue =
        decode0(st, resolveReaderByName(name, reader))

    }

  implicit def decoder[V <: ValueVector, A: Schema](deriver: Deriver[ValueVectorDecoder[V, *]])(implicit
    factory: Factory[A]
  ): ValueVectorDecoder[V, A] =
    factory.derive(deriver)

  implicit val stringDecoder: ValueVectorDecoder[VarCharVector, String]                           =
    decoder[VarCharVector, String](ValueVectorDecoderDeriver.default)
  implicit val boolDecoder: ValueVectorDecoder[BitVector, Boolean]                                =
    decoder[BitVector, Boolean](ValueVectorDecoderDeriver.default)
  implicit val byteDecoder: ValueVectorDecoder[UInt1Vector, Byte]                                 =
    decoder[UInt1Vector, Byte](ValueVectorDecoderDeriver.default)
  implicit val shortDecoder: ValueVectorDecoder[SmallIntVector, Short]                            =
    decoder[SmallIntVector, Short](ValueVectorDecoderDeriver.default)
  implicit val intDecoder: ValueVectorDecoder[IntVector, Int]                                     =
    decoder[IntVector, Int](ValueVectorDecoderDeriver.default)
  implicit val longDecoder: ValueVectorDecoder[BigIntVector, Long]                                =
    decoder[BigIntVector, Long](ValueVectorDecoderDeriver.default)
  implicit val floatDecoder: ValueVectorDecoder[Float4Vector, Float]                              =
    decoder[Float4Vector, Float](ValueVectorDecoderDeriver.default)
  implicit val doubleDecoder: ValueVectorDecoder[Float8Vector, Double]                            =
    decoder[Float8Vector, Double](ValueVectorDecoderDeriver.default)
  implicit val binaryDecoder: ValueVectorDecoder[LargeVarBinaryVector, Chunk[Byte]]               =
    decoder[LargeVarBinaryVector, Chunk[Byte]](ValueVectorDecoderDeriver.default)
  implicit val charDecoder: ValueVectorDecoder[UInt2Vector, Char]                                 =
    decoder[UInt2Vector, Char](ValueVectorDecoderDeriver.default)
  implicit val uuidDecoder: ValueVectorDecoder[VarBinaryVector, java.util.UUID]                   =
    decoder[VarBinaryVector, java.util.UUID](ValueVectorDecoderDeriver.default)
  implicit val bigDecimalDecoder: ValueVectorDecoder[DecimalVector, java.math.BigDecimal]         =
    decoder[DecimalVector, java.math.BigDecimal](ValueVectorDecoderDeriver.default)
  implicit val bigIntegerDecoder: ValueVectorDecoder[VarBinaryVector, java.math.BigInteger]       =
    decoder[VarBinaryVector, java.math.BigInteger](ValueVectorDecoderDeriver.default)
  implicit val dayOfWeekDecoder: ValueVectorDecoder[IntVector, java.time.DayOfWeek]               =
    decoder[IntVector, java.time.DayOfWeek](ValueVectorDecoderDeriver.default)
  implicit val monthDecoder: ValueVectorDecoder[IntVector, java.time.Month]                       =
    decoder[IntVector, java.time.Month](ValueVectorDecoderDeriver.default)
  implicit val monthDayDecoder: ValueVectorDecoder[BigIntVector, java.time.MonthDay]              =
    decoder[BigIntVector, java.time.MonthDay](ValueVectorDecoderDeriver.default)
  implicit val periodDecoder: ValueVectorDecoder[VarBinaryVector, java.time.Period]               =
    decoder[VarBinaryVector, java.time.Period](ValueVectorDecoderDeriver.default)
  implicit val yearDecoder: ValueVectorDecoder[IntVector, java.time.Year]                         =
    decoder[IntVector, java.time.Year](ValueVectorDecoderDeriver.default)
  implicit val yearMonthDecoder: ValueVectorDecoder[BigIntVector, java.time.YearMonth]            =
    decoder[BigIntVector, java.time.YearMonth](ValueVectorDecoderDeriver.default)
  implicit val zoneIdDecoder: ValueVectorDecoder[VarCharVector, java.time.ZoneId]                 =
    decoder[VarCharVector, java.time.ZoneId](ValueVectorDecoderDeriver.default)
  implicit val zoneOffsetDecoder: ValueVectorDecoder[VarCharVector, java.time.ZoneOffset]         =
    decoder[VarCharVector, java.time.ZoneOffset](ValueVectorDecoderDeriver.default)
  implicit val durationDecoder: ValueVectorDecoder[BigIntVector, java.time.Duration]              =
    decoder[BigIntVector, java.time.Duration](ValueVectorDecoderDeriver.default)
  implicit val instantDecoder: ValueVectorDecoder[BigIntVector, java.time.Instant]                =
    decoder[BigIntVector, java.time.Instant](ValueVectorDecoderDeriver.default)
  implicit val localDateDecoder: ValueVectorDecoder[VarCharVector, java.time.LocalDate]           =
    decoder[VarCharVector, java.time.LocalDate](ValueVectorDecoderDeriver.default)
  implicit val localTimeDecoder: ValueVectorDecoder[VarCharVector, java.time.LocalTime]           =
    decoder[VarCharVector, java.time.LocalTime](ValueVectorDecoderDeriver.default)
  implicit val localDateTimeDecoder: ValueVectorDecoder[VarCharVector, java.time.LocalDateTime]   =
    decoder[VarCharVector, java.time.LocalDateTime](ValueVectorDecoderDeriver.default)
  implicit val offsetTimeDecoder: ValueVectorDecoder[VarCharVector, java.time.OffsetTime]         =
    decoder[VarCharVector, java.time.OffsetTime](ValueVectorDecoderDeriver.default)
  implicit val offsetDateTimeDecoder: ValueVectorDecoder[VarCharVector, java.time.OffsetDateTime] =
    decoder[VarCharVector, java.time.OffsetDateTime](ValueVectorDecoderDeriver.default)
  implicit val zonedDateTimeDecoder: ValueVectorDecoder[VarCharVector, java.time.ZonedDateTime]   =
    decoder[VarCharVector, java.time.ZonedDateTime](ValueVectorDecoderDeriver.default)

  implicit def listDecoder[A, C[_]](implicit
    factory: Factory[C[A]],
    schema: Schema[C[A]]
  ): ValueVectorDecoder[ListVector, C[A]] =
    listDecoderFromDefaultDeriver[A, C]

  implicit def listChunkDecoder[A](implicit
    factory: Factory[Chunk[A]],
    schema: Schema[Chunk[A]]
  ): ValueVectorDecoder[ListVector, Chunk[A]] =
    listDecoder[A, Chunk]

  implicit def listOptionDecoder[A, C[_]](implicit
    factory: Factory[C[Option[A]]],
    schema: Schema[C[Option[A]]]
  ): ValueVectorDecoder[ListVector, C[Option[A]]] =
    listDecoder[Option[A], C]

  implicit def listChunkOptionDecoder[A](implicit
    factory: Factory[Chunk[Option[A]]],
    schema: Schema[Chunk[Option[A]]]
  ): ValueVectorDecoder[ListVector, Chunk[Option[A]]] =
    listChunkDecoder[Option[A]]

  def listDecoderFromDeriver[A, C[_]](
    deriver: Deriver[ValueVectorDecoder[ListVector, *]]
  )(implicit factory: Factory[C[A]], schema: Schema[C[A]]): ValueVectorDecoder[ListVector, C[A]] =
    factory.derive[ValueVectorDecoder[ListVector, *]](deriver)

  def listDecoderFromDefaultDeriver[A, C[_]](implicit
    factory: Factory[C[A]],
    schema: Schema[C[A]]
  ): ValueVectorDecoder[ListVector, C[A]] =
    listDecoderFromDeriver[A, C](ValueVectorDecoderDeriver.default[ListVector])

  def listDecoderFromSummonedDeriver[A, C[_]](implicit
    factory: Factory[C[A]],
    schema: Schema[C[A]]
  ): ValueVectorDecoder[ListVector, C[A]] =
    listDecoderFromDeriver(ValueVectorDecoderDeriver.summoned[ListVector])

  implicit def structDecoder[A](implicit
    factory: Factory[A],
    schema: Schema[A]
  ): ValueVectorDecoder[StructVector, A] =
    structDecoderFromDefaultDeriver[A]

  def structDecoderFromDeriver[A](
    deriver: Deriver[ValueVectorDecoder[StructVector, *]]
  )(implicit factory: Factory[A], schema: Schema[A]): ValueVectorDecoder[StructVector, A] =
    factory.derive[ValueVectorDecoder[StructVector, *]](deriver)

  def structDecoderFromDefaultDeriver[A](implicit
    factory: Factory[A],
    schema: Schema[A]
  ): ValueVectorDecoder[StructVector, A] =
    structDecoderFromDeriver(ValueVectorDecoderDeriver.default[StructVector])

  implicit def optionDecoder[V <: ValueVector, A](implicit
    factory: Factory[Option[A]],
    schema: Schema[Option[A]]
  ): ValueVectorDecoder[V, Option[A]] =
    optionDecoderFromDefaultDeriver[V, A]

  implicit def optionListDecoder[A, C[_]](implicit
    factory: Factory[Option[C[A]]],
    schema: Schema[Option[C[A]]]
  ): ValueVectorDecoder[ListVector, Option[C[A]]] =
    optionDecoder[ListVector, C[A]]

  implicit def optionListChunkDecoder[A](implicit
    factory: Factory[Option[Chunk[A]]],
    schema: Schema[Option[Chunk[A]]]
  ): ValueVectorDecoder[ListVector, Option[Chunk[A]]] =
    optionDecoder[ListVector, Chunk[A]]

  def optionDecoderFromDeriver[V <: ValueVector, A](
    deriver: Deriver[ValueVectorDecoder[V, *]]
  )(implicit factory: Factory[Option[A]], schema: Schema[Option[A]]): ValueVectorDecoder[V, Option[A]] =
    factory.derive[ValueVectorDecoder[V, *]](deriver)

  def optionDecoderFromDefaultDeriver[V <: ValueVector, A](implicit
    factory: Factory[Option[A]],
    schema: Schema[Option[A]]
  ): ValueVectorDecoder[V, Option[A]] =
    optionDecoderFromDeriver(ValueVectorDecoderDeriver.default[V])

}
