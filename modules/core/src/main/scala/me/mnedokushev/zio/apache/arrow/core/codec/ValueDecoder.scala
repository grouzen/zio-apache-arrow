package me.mnedokushev.zio.apache.arrow.core.codec

import org.apache.arrow.vector.complex.reader.FieldReader
import zio.schema.{ DynamicValue, _ }
import zio.{ Duration, _ }

import java.nio.ByteBuffer
import java.time._
import java.util.UUID
import scala.collection.immutable.ListMap
import org.apache.arrow.vector.ValueVector

trait ValueDecoder[+A] {

  def decodeValue[V0 <: ValueVector](name: Option[String], reader: FieldReader, vec: V0, idx: Int): DynamicValue

}

object ValueDecoder {

  private[codec] def decodeStruct[V0 <: ValueVector, A](
    fields: Chunk[Schema.Field[A, _]],
    decoders: Chunk[ValueDecoder[_]],
    reader: FieldReader,
    vec: V0,
    idx: Int
  ): DynamicValue = {
    val values = ListMap(fields.zip(decoders).map { case (field, decoder) =>
      val value: DynamicValue = decoder.decodeValue(Some(field.name), reader, vec, idx)

      field.name.toString -> value
    }: _*)

    DynamicValue.Record(TypeId.Structural, values)
  }

  private[codec] def decodeList[V0 <: ValueVector, A](
    decoder: ValueDecoder[A],
    reader: FieldReader,
    vec: V0,
    idx: Int
  ): DynamicValue = {
    val builder = ChunkBuilder.make[DynamicValue]()
    var idx0    = idx

    while (reader.next()) {
      builder.addOne(decoder.decodeValue(None, reader, vec, idx0))
      idx0 += 1
    }

    DynamicValue.Sequence(builder.result())
  }

  private[codec] def decodePrimitive[A](standardType: StandardType[A], reader: FieldReader): DynamicValue =
    standardType match {
      case t: StandardType.StringType.type         =>
        DynamicValue.Primitive[String](reader.readText().toString, t)
      case t: StandardType.BoolType.type           =>
        DynamicValue.Primitive[Boolean](reader.readBoolean(), t)
      case t: StandardType.ByteType.type           =>
        DynamicValue.Primitive[Byte](reader.readByte(), t)
      case t: StandardType.ShortType.type          =>
        DynamicValue.Primitive[Short](reader.readShort(), t)
      case t: StandardType.IntType.type            =>
        DynamicValue.Primitive[Int](reader.readInteger(), t)
      case t: StandardType.LongType.type           =>
        DynamicValue.Primitive[Long](reader.readLong(), t)
      case t: StandardType.FloatType.type          =>
        DynamicValue.Primitive[Float](reader.readFloat(), t)
      case t: StandardType.DoubleType.type         =>
        DynamicValue.Primitive[Double](reader.readDouble(), t)
      case t: StandardType.BinaryType.type         =>
        DynamicValue.Primitive[Chunk[Byte]](Chunk.fromArray(reader.readByteArray()), t)
      case t: StandardType.CharType.type           =>
        DynamicValue.Primitive[Char](reader.readCharacter(), t)
      case t: StandardType.UUIDType.type           =>
        val bb = ByteBuffer.wrap(reader.readByteArray())
        DynamicValue.Primitive[UUID](new UUID(bb.getLong, bb.getLong), t)
      case t: StandardType.BigDecimalType.type     =>
        DynamicValue.Primitive[java.math.BigDecimal](reader.readBigDecimal(), t)
      case t: StandardType.BigIntegerType.type     =>
        DynamicValue.Primitive[java.math.BigInteger](new java.math.BigInteger(reader.readByteArray()), t)
      case t: StandardType.DayOfWeekType.type      =>
        DynamicValue.Primitive[DayOfWeek](DayOfWeek.of(reader.readInteger()), t)
      case t: StandardType.MonthType.type          =>
        DynamicValue.Primitive[Month](Month.of(reader.readInteger()), t)
      case t: StandardType.MonthDayType.type       =>
        val bb = ByteBuffer.allocate(8).putLong(reader.readLong())
        DynamicValue.Primitive[MonthDay](MonthDay.of(bb.getInt(0), bb.getInt(4)), t)
      case t: StandardType.PeriodType.type         =>
        val bb = ByteBuffer.wrap(reader.readByteArray())
        DynamicValue.Primitive[Period](Period.of(bb.getInt(0), bb.getInt(4), bb.getInt(8)), t)
      case t: StandardType.YearType.type           =>
        DynamicValue.Primitive[Year](Year.of(reader.readInteger()), t)
      case t: StandardType.YearMonthType.type      =>
        val bb = ByteBuffer.allocate(8).putLong(reader.readLong())
        DynamicValue.Primitive[YearMonth](YearMonth.of(bb.getInt(0), bb.getInt(4)), t)
      case t: StandardType.ZoneIdType.type         =>
        DynamicValue.Primitive[ZoneId](ZoneId.of(reader.readText().toString), t)
      case t: StandardType.ZoneOffsetType.type     =>
        DynamicValue.Primitive[ZoneOffset](ZoneOffset.of(reader.readText().toString), t)
      case t: StandardType.DurationType.type       =>
        DynamicValue.Primitive[Duration](Duration.fromMillis(reader.readLong()), t)
      case t: StandardType.InstantType.type        =>
        DynamicValue.Primitive[Instant](Instant.ofEpochMilli(reader.readLong()), t)
      case t: StandardType.LocalDateType.type      =>
        DynamicValue.Primitive[LocalDate](LocalDate.parse(reader.readText().toString), t)
      case t: StandardType.LocalTimeType.type      =>
        DynamicValue.Primitive[LocalTime](LocalTime.parse(reader.readText().toString), t)
      case t: StandardType.LocalDateTimeType.type  =>
        DynamicValue.Primitive[LocalDateTime](LocalDateTime.parse(reader.readText().toString), t)
      case t: StandardType.OffsetTimeType.type     =>
        DynamicValue.Primitive[OffsetTime](OffsetTime.parse(reader.readText().toString), t)
      case t: StandardType.OffsetDateTimeType.type =>
        DynamicValue.Primitive[OffsetDateTime](OffsetDateTime.parse(reader.readText().toString), t)
      case t: StandardType.ZonedDateTimeType.type  =>
        DynamicValue.Primitive[ZonedDateTime](ZonedDateTime.parse(reader.readText().toString), t)
      case other                                   =>
        throw DecoderError(s"Unsupported ZIO Schema type $other")
    }

}
