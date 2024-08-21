package me.mnedokushev.zio.apache.arrow.core.codec

import org.apache.arrow.memory.{ ArrowBuf, BufferAllocator }
import org.apache.arrow.vector.complex.writer.{ FieldWriter, _ }
import zio.Chunk
import zio.schema.{ Schema, StandardType }

import java.nio.charset.StandardCharsets
import java.time._
import java.util.UUID

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

  private[codec] def encodePrimitive0[A](
    nested: Boolean,
    standardType: StandardType[A],
    value: A,
    writer: FieldWriter,
    name: Option[String]
  )(implicit alloc: BufferAllocator): Unit = {

    def resolveWriter[W <: BaseWriter](f1: => W)(f2: String => W): W =
      if (nested)
        name.fold(f1)(f2)
      else
        writer.asInstanceOf[W]

    def withBuffer(size: Long)(fn: ArrowBuf => Unit) = {
      val buffer = alloc.buffer(size)
      fn(buffer)
      buffer.close()
    }

    def writeString(s: String) =
      withBuffer(s.length.toLong) { buffer =>
        buffer.writeBytes(s.getBytes(StandardCharsets.UTF_8))
        resolveWriter(writer.varChar)(writer.varChar).writeVarChar(0, s.length, buffer)
      }

    def writeLong[A1](v: A1)(fst: A1 => Int)(snd: A1 => Int) =
      withBuffer(8) { buffer =>
        buffer.writeInt(fst(v))
        buffer.writeInt(snd(v))
        resolveWriter(writer.bigInt)(writer.bigInt).writeBigInt(buffer.getLong(0))
      }

    (standardType, value) match {
      case (StandardType.StringType, v: String)                   =>
        writeString(v)
      case (StandardType.BoolType, v: Boolean)                    =>
        resolveWriter(writer.bit)(writer.bit).writeBit(if (v) 1 else 0)
      case (StandardType.ByteType, v: Byte)                       =>
        resolveWriter(writer.uInt1)(writer.uInt1).writeUInt1(v)
      case (StandardType.ShortType, v: Short)                     =>
        resolveWriter(writer.smallInt)(writer.smallInt).writeSmallInt(v)
      case (StandardType.IntType, v: Int)                         =>
        resolveWriter(writer.integer)(writer.integer).writeInt(v)
      case (StandardType.LongType, v: Long)                       =>
        resolveWriter(writer.bigInt)(writer.bigInt).writeBigInt(v)
      case (StandardType.FloatType, v: Float)                     =>
        resolveWriter(writer.float4)(writer.float4).writeFloat4(v)
      case (StandardType.DoubleType, v: Double)                   =>
        resolveWriter(writer.float8)(writer.float8).writeFloat8(v)
      case (StandardType.BinaryType, v: Chunk[_])                 =>
        withBuffer(v.length.toLong) { buffer =>
          buffer.writeBytes(v.asInstanceOf[Chunk[Byte]].toArray)
          resolveWriter(writer.largeVarBinary)(writer.largeVarBinary).writeLargeVarBinary(0L, v.length.toLong, buffer)
        }
      case (StandardType.CharType, v: Char)                       =>
        resolveWriter(writer.uInt2)(writer.uInt2).writeUInt2(v)
      case (StandardType.UUIDType, v: UUID)                       =>
        withBuffer(16) { buffer =>
          buffer.writeLong(v.getMostSignificantBits)
          buffer.writeLong(v.getLeastSignificantBits)
          resolveWriter(writer.varBinary)(writer.varBinary).writeVarBinary(0, 16, buffer)
        }
      case (StandardType.BigDecimalType, v: java.math.BigDecimal) =>
        resolveWriter(writer.decimal)(writer.decimal).writeDecimal(v)
      case (StandardType.BigIntegerType, v: java.math.BigInteger) =>
        val bb = v.toByteArray
        withBuffer(bb.length.toLong) { buffer =>
          buffer.writeBytes(bb)
          resolveWriter(writer.varBinary)(writer.varBinary).writeVarBinary(0, bb.length, buffer)
        }
      case (StandardType.DayOfWeekType, v: DayOfWeek)             =>
        resolveWriter(writer.integer)(writer.integer).writeInt(v.getValue)
      case (StandardType.MonthType, v: Month)                     =>
        resolveWriter(writer.integer)(writer.integer).writeInt(v.getValue)
      case (StandardType.MonthDayType, v: MonthDay)               =>
        writeLong(v)(_.getDayOfMonth)(_.getMonthValue)
      case (StandardType.PeriodType, v: Period)                   =>
        withBuffer(12) { buffer =>
          buffer.writeInt(v.getDays)
          buffer.writeInt(v.getMonths)
          buffer.writeInt(v.getYears)
          resolveWriter(writer.varBinary)(writer.varBinary).writeVarBinary(0, 12, buffer)
        }
      case (StandardType.YearType, v: Year)                       =>
        resolveWriter(writer.integer)(writer.integer).writeInt(v.getValue)
      case (StandardType.YearMonthType, v: YearMonth)             =>
        writeLong(v)(_.getMonthValue)(_.getYear)
      case (StandardType.ZoneIdType, v: ZoneId)                   =>
        writeString(v.toString)
      case (StandardType.ZoneOffsetType, v: ZoneOffset)           =>
        writeString(v.toString)
      case (StandardType.DurationType, v: Duration)               =>
        resolveWriter(writer.bigInt)(writer.bigInt).writeBigInt(v.toMillis)
      case (StandardType.InstantType, v: Instant)                 =>
        resolveWriter(writer.bigInt)(writer.bigInt).writeBigInt(v.toEpochMilli)
      case (StandardType.LocalDateType, v: LocalDate)             =>
        writeString(v.toString)
      case (StandardType.LocalTimeType, v: LocalTime)             =>
        writeString(v.toString)
      case (StandardType.LocalDateTimeType, v: LocalDateTime)     =>
        writeString(v.toString)
      case (StandardType.OffsetTimeType, v: OffsetTime)           =>
        writeString(v.toString)
      case (StandardType.OffsetDateTimeType, v: OffsetDateTime)   =>
        writeString(v.toString)
      case (StandardType.ZonedDateTimeType, v: ZonedDateTime)     =>
        writeString(v.toString)
      case (other, _)                                             =>
        throw EncoderError(s"Unsupported ZIO Schema StandardType $other")
    }

  }

  private[codec] def encodePrimitive[A](
    standardType: StandardType[A],
    value: A,
    name: Option[String],
    writer: FieldWriter
  )(implicit alloc: BufferAllocator): Unit =
    encodePrimitive0(nested = true, standardType, value, writer, name)

  private[codec] def encodePrimitive[A](
    standardType: StandardType[A],
    value: A,
    writer: FieldWriter
  )(implicit alloc: BufferAllocator): Unit =
    encodePrimitive0(nested = false, standardType, value, writer, name = None)

}
