package me.mnedokushev.zio.apache.arrow.core.codec

import org.apache.arrow.memory.{ ArrowBuf, BufferAllocator }
import org.apache.arrow.vector.complex.writer.FieldWriter
// import org.apache.arrow.vector.{ ValueVector, _ }
import zio.Chunk
import zio.schema.{ Schema, StandardType }

// import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.time._
import java.util.UUID
import org.apache.arrow.vector.complex.writer._

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

  // private[codec] def encodePrimitive[A, V <: ValueVector](
  //   standardType: StandardType[A],
  //   value: A,
  //   vec: V,
  //   idx: Int
  // ): Unit =
  //   (standardType, vec, value) match {
  //     case (StandardType.StringType, vec: VarCharVector, v: String)                     =>
  //       vec.set(idx, v.getBytes(StandardCharsets.UTF_8))
  //     case (StandardType.BoolType, vec: BitVector, v: Boolean)                          =>
  //       vec.set(idx, if (v) 1 else 0)
  //     case (StandardType.ByteType, vec: UInt1Vector, v: Byte)                           =>
  //       vec.set(idx, v)
  //     case (StandardType.ShortType, vec: SmallIntVector, v: Short)                      =>
  //       vec.set(idx, v)
  //     case (StandardType.IntType, vec: IntVector, v: Int)                               =>
  //       vec.set(idx, v)
  //     case (StandardType.LongType, vec: BigIntVector, v: Long)                          =>
  //       vec.set(idx, v)
  //     case (StandardType.FloatType, vec: Float4Vector, v: Float)                        =>
  //       vec.set(idx, v)
  //     case (StandardType.DoubleType, vec: Float8Vector, v: Double)                      =>
  //       vec.set(idx, v)
  //     case (StandardType.BinaryType, vec: LargeVarBinaryVector, v: Chunk[_])            =>
  //       vec.set(idx, v.asInstanceOf[Chunk[Byte]].toArray)
  //     case (StandardType.CharType, vec: UInt2Vector, v: Char)                           =>
  //       vec.set(idx, v)
  //     case (StandardType.UUIDType, vec: VarBinaryVector, v: UUID)                       =>
  //       val bb = ByteBuffer.allocate(16)
  //       bb.putLong(v.getMostSignificantBits)
  //       bb.putLong(v.getLeastSignificantBits)
  //       vec.set(idx, bb.array())
  //     case (StandardType.BigDecimalType, vec: DecimalVector, v: java.math.BigDecimal)   =>
  //       vec.set(idx, v)
  //     case (StandardType.BigIntegerType, vec: VarBinaryVector, v: java.math.BigInteger) =>
  //       vec.set(idx, v.toByteArray)
  //     case (StandardType.DayOfWeekType, vec: IntVector, v: DayOfWeek)                   =>
  //       vec.set(idx, v.getValue)
  //     case (StandardType.MonthType, vec: IntVector, v: Month)                           =>
  //       vec.set(idx, v.getValue)
  //     case (StandardType.MonthDayType, vec: BigIntVector, v: MonthDay)                  =>
  //       val bb = ByteBuffer.allocate(8)
  //       bb.putInt(v.getMonthValue)
  //       bb.putInt(v.getDayOfMonth)
  //       vec.set(idx, bb.getLong(0))
  //     case (StandardType.PeriodType, vec: VarBinaryVector, v: Period)                   =>
  //       val bb = ByteBuffer.allocate(12)
  //       bb.putInt(v.getYears)
  //       bb.putInt(v.getMonths)
  //       bb.putInt(v.getDays)
  //       vec.set(idx, bb.array())
  //     case (StandardType.YearType, vec: IntVector, v: Year)                             =>
  //       vec.set(idx, v.getValue)
  //     case (StandardType.YearMonthType, vec: BigIntVector, v: YearMonth)                =>
  //       val bb = ByteBuffer.allocate(8)
  //       bb.putInt(v.getYear)
  //       bb.putInt(v.getMonthValue)
  //       vec.set(idx, bb.getLong(0))
  //     case (StandardType.ZoneIdType, vec: VarCharVector, v: ZoneId)                     =>
  //       vec.set(idx, v.toString.getBytes(StandardCharsets.UTF_8))
  //     case (StandardType.ZoneOffsetType, vec: VarCharVector, v: ZoneOffset)             =>
  //       vec.set(idx, v.toString.getBytes(StandardCharsets.UTF_8))
  //     case (StandardType.DurationType, vec: BigIntVector, v: Duration)                  =>
  //       vec.set(idx, v.toMillis)
  //     case (StandardType.InstantType, vec: BigIntVector, v: Instant)                    =>
  //       vec.set(idx, v.toEpochMilli)
  //     case (StandardType.LocalDateType, vec: VarCharVector, v: LocalDate)               =>
  //       vec.set(idx, v.toString.getBytes(StandardCharsets.UTF_8))
  //     case (StandardType.LocalTimeType, vec: VarCharVector, v: LocalTime)               =>
  //       vec.set(idx, v.toString.getBytes(StandardCharsets.UTF_8))
  //     case (StandardType.LocalDateTimeType, vec: VarCharVector, v: LocalDateTime)       =>
  //       vec.set(idx, v.toString.getBytes(StandardCharsets.UTF_8))
  //     case (StandardType.OffsetTimeType, vec: VarCharVector, v: OffsetTime)             =>
  //       vec.set(idx, v.toString.getBytes(StandardCharsets.UTF_8))
  //     case (StandardType.OffsetDateTimeType, vec: VarCharVector, v: OffsetDateTime)     =>
  //       vec.set(idx, v.toString.getBytes(StandardCharsets.UTF_8))
  //     case (StandardType.ZonedDateTimeType, vec: VarCharVector, v: ZonedDateTime)       =>
  //       vec.set(idx, v.toString.getBytes(StandardCharsets.UTF_8))
  //     case (other, _, _)                                                                =>
  //       throw EncoderError(s"Unsupported ZIO Schema StandardType $other")
  //   }

  private[codec] def encodePrimitive[A](
    standardType: StandardType[A],
    value: A,
    name: Option[String],
    writer: FieldWriter
  )(implicit alloc: BufferAllocator): Unit = {

    def withBuffer(size: Long)(fn: ArrowBuf => Unit) = {
      val buffer = alloc.buffer(size)
      fn(buffer)
      buffer.close()
    }

    def writeString(s: String) =
      withBuffer(s.length.toLong) { buffer =>
        buffer.writeBytes(s.getBytes(StandardCharsets.UTF_8))
        name.fold(writer.varChar)(writer.varChar).writeVarChar(0, s.length, buffer)
      }

    def writeLong[A1](v: A1)(fst: A1 => Int)(snd: A1 => Int) =
      withBuffer(8) { buffer =>
        buffer.writeInt(fst(v))
        buffer.writeInt(snd(v))
        name.fold(writer.bigInt)(writer.bigInt).writeBigInt(buffer.getLong(0))
      }

    (standardType, value) match {
      case (StandardType.StringType, v: String)                   =>
        writeString(v)
      case (StandardType.BoolType, v: Boolean)                    =>
        name.fold(writer.bit)(writer.bit).writeBit(if (v) 1 else 0)
      case (StandardType.ByteType, v: Byte)                       =>
        name.fold(writer.uInt1)(writer.uInt1).writeUInt1(v)
      case (StandardType.ShortType, v: Short)                     =>
        name.fold(writer.smallInt)(writer.smallInt).writeSmallInt(v)
      case (StandardType.IntType, v: Int)                         =>
        name.fold(writer.integer)(writer.integer).writeInt(v)
      case (StandardType.LongType, v: Long)                       =>
        name.fold(writer.bigInt)(writer.bigInt).writeBigInt(v)
      case (StandardType.FloatType, v: Float)                     =>
        name.fold(writer.float4)(writer.float4).writeFloat4(v)
      case (StandardType.DoubleType, v: Double)                   =>
        name.fold(writer.float8)(writer.float8).writeFloat8(v)
      case (StandardType.BinaryType, v: Chunk[_])                 =>
        withBuffer(v.length.toLong) { buffer =>
          buffer.writeBytes(v.asInstanceOf[Chunk[Byte]].toArray)
          name.fold(writer.largeVarBinary)(writer.largeVarBinary).writeLargeVarBinary(0L, v.length.toLong, buffer)
        }
      case (StandardType.CharType, v: Char)                       =>
        name.fold(writer.uInt2)(writer.uInt2).writeUInt2(v)
      case (StandardType.UUIDType, v: UUID)                       =>
        withBuffer(16) { buffer =>
          buffer.writeLong(v.getLeastSignificantBits)
          buffer.writeLong(v.getMostSignificantBits)
          name.fold(writer.varBinary)(writer.varBinary).writeVarBinary(0, 16, buffer)
        }
      case (StandardType.BigDecimalType, v: java.math.BigDecimal) =>
        name.fold(writer.decimal)(writer.decimal).writeDecimal(v)
      case (StandardType.BigIntegerType, v: java.math.BigInteger) =>
        val bb = v.toByteArray
        withBuffer(bb.length.toLong) { buffer =>
          buffer.writeBytes(bb)
          name.fold(writer.varBinary)(writer.varBinary).writeVarBinary(0, bb.length, buffer)
        }
      case (StandardType.DayOfWeekType, v: DayOfWeek)             =>
        name.fold(writer.integer)(writer.integer).writeInt(v.getValue)
      case (StandardType.MonthType, v: Month)                     =>
        name.fold(writer.integer)(writer.integer).writeInt(v.getValue)
      case (StandardType.MonthDayType, v: MonthDay)               =>
        writeLong(v)(_.getDayOfMonth)(_.getMonthValue)
      case (StandardType.PeriodType, v: Period)                   =>
        withBuffer(12) { buffer =>
          buffer.writeInt(v.getDays)
          buffer.writeInt(v.getMonths)
          buffer.writeInt(v.getYears)
          name.fold(writer.varBinary)(writer.varBinary).writeVarBinary(0, 12, buffer)
        }
      case (StandardType.YearType, v: Year)                       =>
        name.fold(writer.integer)(writer.integer).writeInt(v.getValue)
      case (StandardType.YearMonthType, v: YearMonth)             =>
        writeLong(v)(_.getMonthValue)(_.getYear)
      case (StandardType.ZoneIdType, v: ZoneId)                   =>
        writeString(v.toString)
      case (StandardType.ZoneOffsetType, v: ZoneOffset)           =>
        writeString(v.toString)
      case (StandardType.DurationType, v: Duration)               =>
        name.fold(writer.bigInt)(writer.bigInt).writeBigInt(v.toMillis)
      case (StandardType.InstantType, v: Instant)                 =>
        name.fold(writer.bigInt)(writer.bigInt).writeBigInt(v.toEpochMilli)
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
    writer: FieldWriter
  )(implicit alloc: BufferAllocator): Unit = {

    def withBuffer(size: Long)(fn: ArrowBuf => Unit) = {
      val buffer = alloc.buffer(size)
      fn(buffer)
      buffer.close()
    }

    def writeString(s: String, writer0: VarCharWriter) =
      withBuffer(s.length.toLong) { buffer =>
        buffer.writeBytes(s.getBytes(StandardCharsets.UTF_8))
        writer0.writeVarChar(0, s.length, buffer)
      }

    def writeLong[A1](v: A1, writer0: BigIntWriter)(fst: A1 => Int)(snd: A1 => Int) =
      withBuffer(8) { buffer =>
        buffer.writeInt(fst(v))
        buffer.writeInt(snd(v))
        writer0.writeBigInt(buffer.getLong(0))
      }

    (standardType, value, writer) match {
      case (StandardType.StringType, v: String, w: VarCharWriter)                     =>
        writeString(v, w)
      case (StandardType.BoolType, v: Boolean, w: BitWriter)                          =>
        w.writeBit(if (v) 1 else 0)
      case (StandardType.ByteType, v: Byte, w: UInt1Writer)                           =>
        w.writeUInt1(v)
      case (StandardType.ShortType, v: Short, w: SmallIntWriter)                      =>
        w.writeSmallInt(v)
      case (StandardType.IntType, v: Int, w: IntWriter)                               =>
        w.writeInt(v)
      case (StandardType.LongType, v: Long, w: BigIntWriter)                          =>
        w.writeBigInt(v)
      case (StandardType.FloatType, v: Float, w: Float4Writer)                        =>
        w.writeFloat4(v)
      case (StandardType.DoubleType, v: Double, w: Float8Writer)                      =>
        w.writeFloat8(v)
      case (StandardType.BinaryType, v: Chunk[_], w: LargeVarBinaryWriter)            =>
        withBuffer(v.length.toLong) { buffer =>
          buffer.writeBytes(v.asInstanceOf[Chunk[Byte]].toArray)
          w.writeLargeVarBinary(0L, v.length.toLong, buffer)
        }
      case (StandardType.CharType, v: Char, w: UInt2Writer)                           =>
        w.writeUInt2(v)
      case (StandardType.UUIDType, v: UUID, w: VarBinaryWriter)                       =>
        withBuffer(16) { buffer =>
          buffer.writeLong(v.getLeastSignificantBits)
          buffer.writeLong(v.getMostSignificantBits)
          w.writeVarBinary(0, 16, buffer)
        }
      case (StandardType.BigDecimalType, v: java.math.BigDecimal, w: DecimalWriter)   =>
        w.writeDecimal(v)
      case (StandardType.BigIntegerType, v: java.math.BigInteger, w: VarBinaryWriter) =>
        val bb = v.toByteArray
        withBuffer(bb.length.toLong) { buffer =>
          buffer.writeBytes(bb)
          w.writeVarBinary(0, bb.length, buffer)
        }
      case (StandardType.DayOfWeekType, v: DayOfWeek, w: IntWriter)                   =>
        w.writeInt(v.getValue)
      case (StandardType.MonthType, v: Month, w: IntWriter)                           =>
        w.writeInt(v.getValue)
      case (StandardType.MonthDayType, v: MonthDay, w: BigIntWriter)                  =>
        writeLong(v, w)(_.getDayOfMonth)(_.getMonthValue)
      case (StandardType.PeriodType, v: Period, w: VarBinaryWriter)                   =>
        withBuffer(12) { buffer =>
          buffer.writeInt(v.getDays)
          buffer.writeInt(v.getMonths)
          buffer.writeInt(v.getYears)
          w.writeVarBinary(0, 12, buffer)
        }
      case (StandardType.YearType, v: Year, w: IntWriter)                             =>
        w.writeInt(v.getValue)
      case (StandardType.YearMonthType, v: YearMonth, w: BigIntWriter)                =>
        writeLong(v, w)(_.getMonthValue)(_.getYear)
      case (StandardType.ZoneIdType, v: ZoneId, w: VarCharWriter)                     =>
        writeString(v.toString, w)
      case (StandardType.ZoneOffsetType, v: ZoneOffset, w: VarCharWriter)             =>
        writeString(v.toString, w)
      case (StandardType.DurationType, v: Duration, w: BigIntWriter)                  =>
        w.writeBigInt(v.toMillis)
      case (StandardType.InstantType, v: Instant, w: BigIntWriter)                    =>
        w.writeBigInt(v.toEpochMilli)
      case (StandardType.LocalDateType, v: LocalDate, w: VarCharWriter)               =>
        writeString(v.toString, w)
      case (StandardType.LocalTimeType, v: LocalTime, w: VarCharWriter)               =>
        writeString(v.toString, w)
      case (StandardType.LocalDateTimeType, v: LocalDateTime, w: VarCharWriter)       =>
        writeString(v.toString, w)
      case (StandardType.OffsetTimeType, v: OffsetTime, w: VarCharWriter)             =>
        writeString(v.toString, w)
      case (StandardType.OffsetDateTimeType, v: OffsetDateTime, w: VarCharWriter)     =>
        writeString(v.toString, w)
      case (StandardType.ZonedDateTimeType, v: ZonedDateTime, w: VarCharWriter)       =>
        writeString(v.toString, w)
      case (other, _, _)                                                              =>
        throw EncoderError(s"Unsupported ZIO Schema StandardType $other")
    }
  }

}
