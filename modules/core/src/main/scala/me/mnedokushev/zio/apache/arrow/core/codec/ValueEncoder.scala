package me.mnedokushev.zio.apache.arrow.core.codec

import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.complex.writer.FieldWriter
import zio.Chunk
import org.apache.arrow.memory.BufferAllocator
import zio.schema.StandardType
import org.apache.arrow.vector._
import java.nio.ByteBuffer
import java.util.UUID
import java.time._
import java.nio.charset.StandardCharsets
import zio.schema.Schema

trait ValueEncoder[-A] {

  def encodeValue(
    value: A,
    name: Option[String],
    writer: FieldWriter
  )(implicit alloc: BufferAllocator): Unit

}

object ValueEncoder {

  private[codec] def encodeStruct[A](
    value: A,
    fields: Chunk[Schema.Field[A, _]],
    encoders: Chunk[ValueEncoder[_]],
    writer: FieldWriter
  )(implicit alloc: BufferAllocator): Unit = {

    def encodeValue0[A1](
      encoder: ValueEncoder[A1],
      value: A,
      get: A => Any,
      name: Option[String],
      writer: FieldWriter
    )(implicit alloc: BufferAllocator) =
      encoder.encodeValue(get(value).asInstanceOf[A1], name, writer)

    writer.start()
    fields.zip(encoders).foreach { case (Schema.Field(name, _, _, _, get, _), encoder) =>
      encodeValue0(encoder, value, get, Some(name), writer)
    }
    writer.end()
  }

  private[codec] def encodeList[A](
    chunk: Chunk[A],
    encoder: ValueEncoder[A],
    writer: FieldWriter
  )(implicit alloc: BufferAllocator): Unit = {
    val it = chunk.iterator

    writer.startList()
    it.foreach(encoder.encodeValue(_, None, writer))
    writer.endList()
  }

  private[codec] def encodePrimitive[A, V <: ValueVector](
    standardType: StandardType[A],
    value: A,
    vec: V,
    idx: Int
  ): Unit =
    (standardType, vec, value) match {
      case (StandardType.StringType, vec: VarCharVector, v: String)                     =>
        vec.set(idx, v.getBytes(StandardCharsets.UTF_8))
      case (StandardType.BoolType, vec: BitVector, v: Boolean)                          =>
        vec.set(idx, if (v) 1 else 0)
      case (StandardType.ByteType, vec: UInt1Vector, v: Byte)                           =>
        vec.set(idx, v)
      case (StandardType.ShortType, vec: SmallIntVector, v: Short)                      =>
        vec.set(idx, v)
      case (StandardType.IntType, vec: IntVector, v: Int)                               =>
        vec.set(idx, v)
      case (StandardType.LongType, vec: BigIntVector, v: Long)                          =>
        vec.set(idx, v)
      case (StandardType.FloatType, vec: Float4Vector, v: Float)                        =>
        vec.set(idx, v)
      case (StandardType.DoubleType, vec: Float8Vector, v: Double)                      =>
        vec.set(idx, v)
      case (StandardType.BinaryType, vec: LargeVarBinaryVector, v: Chunk[_])            =>
        vec.set(idx, v.asInstanceOf[Chunk[Byte]].toArray)
      case (StandardType.CharType, vec: UInt2Vector, v: Char)                           =>
        vec.set(idx, v)
      case (StandardType.UUIDType, vec: VarBinaryVector, v: UUID)                       =>
        val bb = ByteBuffer.allocate(16)
        bb.putLong(v.getMostSignificantBits)
        bb.putLong(v.getLeastSignificantBits)
        vec.set(idx, bb.array())
      case (StandardType.BigDecimalType, vec: DecimalVector, v: java.math.BigDecimal)   =>
        vec.set(idx, v)
      case (StandardType.BigIntegerType, vec: VarBinaryVector, v: java.math.BigInteger) =>
        vec.set(idx, v.toByteArray)
      case (StandardType.DayOfWeekType, vec: IntVector, v: DayOfWeek)                   =>
        vec.set(idx, v.getValue)
      case (StandardType.MonthType, vec: IntVector, v: Month)                           =>
        vec.set(idx, v.getValue)
      case (StandardType.MonthDayType, vec: BigIntVector, v: MonthDay)                  =>
        val bb = ByteBuffer.allocate(8)
        bb.putInt(v.getMonthValue)
        bb.putInt(v.getDayOfMonth)
        vec.set(idx, bb.getLong(0))
      case (StandardType.PeriodType, vec: VarBinaryVector, v: Period)                   =>
        val bb = ByteBuffer.allocate(12)
        bb.putInt(v.getYears)
        bb.putInt(v.getMonths)
        bb.putInt(v.getDays)
        vec.set(idx, bb.array())
      case (StandardType.YearType, vec: IntVector, v: Year)                             =>
        vec.set(idx, v.getValue)
      case (StandardType.YearMonthType, vec: BigIntVector, v: YearMonth)                =>
        val bb = ByteBuffer.allocate(8)
        bb.putInt(v.getYear)
        bb.putInt(v.getMonthValue)
        vec.set(idx, bb.getLong(0))
      case (StandardType.ZoneIdType, vec: VarCharVector, v: ZoneId)                     =>
        vec.set(idx, v.toString.getBytes(StandardCharsets.UTF_8))
      case (StandardType.ZoneOffsetType, vec: VarCharVector, v: ZoneOffset)             =>
        vec.set(idx, v.toString.getBytes(StandardCharsets.UTF_8))
      case (StandardType.DurationType, vec: BigIntVector, v: Duration)                  =>
        vec.set(idx, v.toMillis)
      case (StandardType.InstantType, vec: BigIntVector, v: Instant)                    =>
        vec.set(idx, v.toEpochMilli)
      case (StandardType.LocalDateType, vec: VarCharVector, v: LocalDate)               =>
        vec.set(idx, v.toString.getBytes(StandardCharsets.UTF_8))
      case (StandardType.LocalTimeType, vec: VarCharVector, v: LocalTime)               =>
        vec.set(idx, v.toString.getBytes(StandardCharsets.UTF_8))
      case (StandardType.LocalDateTimeType, vec: VarCharVector, v: LocalDateTime)       =>
        vec.set(idx, v.toString.getBytes(StandardCharsets.UTF_8))
      case (StandardType.OffsetTimeType, vec: VarCharVector, v: OffsetTime)             =>
        vec.set(idx, v.toString.getBytes(StandardCharsets.UTF_8))
      case (StandardType.OffsetDateTimeType, vec: VarCharVector, v: OffsetDateTime)     =>
        vec.set(idx, v.toString.getBytes(StandardCharsets.UTF_8))
      case (StandardType.ZonedDateTimeType, vec: VarCharVector, v: ZonedDateTime)       =>
        vec.set(idx, v.toString.getBytes(StandardCharsets.UTF_8))
      case (other, _, _)                                                                =>
        throw EncoderError(s"Unsupported ZIO Schema StandardType $other")
    }

}
