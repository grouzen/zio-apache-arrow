package me.mnedokushev.zio.apache.arrow.core.codec

import org.apache.arrow.memory.{ ArrowBuf, BufferAllocator }
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.impl.{ PromotableWriter, UnionListWriter }
import org.apache.arrow.vector.complex.writer.FieldWriter
import org.apache.arrow.vector.complex.{ ListVector, StructVector }
import zio._
import zio.schema._

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.time.{ DayOfWeek, Month, MonthDay, Period }
import java.util.UUID
import scala.annotation.tailrec
import scala.util.control.NonFatal

trait ValueVectorEncoder[-A, V <: ValueVector] { self =>

  final def encodeZIO(chunk: Chunk[A]): RIO[Scope with BufferAllocator, V] =
    ZIO.fromAutoCloseable(
      ZIO.serviceWithZIO[BufferAllocator] { implicit alloc =>
        ZIO.fromEither(encode(chunk))
      }
    )

  final def encode(chunk: Chunk[A])(implicit alloc: BufferAllocator): Either[Throwable, V] =
    try
      Right(encodeUnsafe(chunk))
    catch {
      case encoderError: EncoderError => Left(encoderError)
      case NonFatal(ex)               => Left(EncoderError("Error encoding vector", Some(ex)))

    }

  protected def encodeUnsafe(chunk: Chunk[A])(implicit alloc: BufferAllocator): V

  final def contramap[B](f: B => A): ValueVectorEncoder[B, V] =
    new ValueVectorEncoder[B, V] {
      override protected def encodeUnsafe(chunk: Chunk[B])(implicit alloc: BufferAllocator): V =
        self.encodeUnsafe(chunk.map(f))
    }

}

object ValueVectorEncoder {

  def apply[A, V <: ValueVector](implicit encoder: ValueVectorEncoder[A, V]): ValueVectorEncoder[A, V] =
    encoder

  implicit def primitive[A, V <: ValueVector](implicit schema: Schema[A]): ValueVectorEncoder[A, V] =
    new ValueVectorEncoder[A, V] {
      override protected def encodeUnsafe(chunk: Chunk[A])(implicit alloc: BufferAllocator): V = {
        def allocate[A1](standardType: StandardType[A1]): V = {
          val vec = standardType match {
            case StandardType.StringType     =>
              new VarCharVector("stringVector", alloc)
            case StandardType.BoolType       =>
              new BitVector("boolVector", alloc)
            case StandardType.ByteType       =>
              new UInt1Vector("byteVector", alloc)
            case StandardType.ShortType      =>
              new SmallIntVector("shortVector", alloc)
            case StandardType.IntType        =>
              new IntVector("intVector", alloc)
            case StandardType.LongType       =>
              new BigIntVector("longVector", alloc)
            case StandardType.FloatType      =>
              new Float4Vector("floatVector", alloc)
            case StandardType.DoubleType     =>
              new Float8Vector("doubleVector", alloc)
            case StandardType.BinaryType     =>
              new LargeVarBinaryVector("binaryVector", alloc)
            case StandardType.CharType       =>
              new UInt2Vector("charVector", alloc)
            case StandardType.UUIDType       =>
              new VarBinaryVector("uuidVector", alloc)
            case StandardType.BigDecimalType =>
              new DecimalVector("bigDecimalVector", alloc, 11, 2)
            case StandardType.BigIntegerType =>
              new VarBinaryVector("bigIntVector", alloc)
            case StandardType.DayOfWeekType  =>
              new IntVector("dayOfWeekVector", alloc)
            case StandardType.MonthType      =>
              new IntVector("monthVector", alloc)
            case StandardType.MonthDayType   =>
              new BigIntVector("monthDayVector", alloc)
            case StandardType.PeriodType     =>
              new VarBinaryVector("period", alloc)
            case other                       =>
              throw EncoderError(s"Unsupported ZIO Schema StandardType $other")
          }

          vec.allocateNew()
          vec.asInstanceOf[V]
        }

        schema match {
          case Schema.Primitive(standardType, _) =>
            val vec = allocate(standardType)
            val len = chunk.length
            val it  = chunk.iterator.zipWithIndex

            it.foreach { case (v, i) =>
              encodePrimitive(v, standardType, vec, i)
            }

            vec.setValueCount(len)
            vec
          case _                                 =>
            throw EncoderError(s"Given ZIO schema must be of type Schema.Primitive[Val]")

        }
      }
    }

  implicit def list[A](implicit schema: Schema[A]): ValueVectorEncoder[Chunk[A], ListVector] =
    new ValueVectorEncoder[Chunk[A], ListVector] {
      override protected def encodeUnsafe(chunk: Chunk[Chunk[A]])(implicit alloc: BufferAllocator): ListVector = {
        val vec    = ListVector.empty("listVector", alloc)
        val len    = chunk.length
        val writer = vec.getWriter
        val it     = chunk.iterator

        it.foreach { vs =>
          writer.startList()
          vs.iterator.foreach(encodeSchema(_, None, schema, writer))
          writer.endList()
        }

        vec.setValueCount(len)
        vec

      }
    }

  implicit def struct[A](implicit schema: Schema[A]): ValueVectorEncoder[A, StructVector] =
    new ValueVectorEncoder[A, StructVector] {
      override protected def encodeUnsafe(chunk: Chunk[A])(implicit alloc: BufferAllocator): StructVector =
        schema match {
          case record: Schema.Record[A] =>
            val vec    = StructVector.empty("structVector", alloc)
            val len    = chunk.length
            val writer = vec.getWriter
            val it     = chunk.iterator.zipWithIndex

            it.foreach { case (v, i) =>
              writer.setPosition(i)
              encodeCaseClass(v, record.fields, writer)
              vec.setIndexDefined(i)
            }
            writer.setValueCount(len)

            vec
          case _                        =>
            throw EncoderError(s"Given ZIO schema must be of type Schema.Record[Val]")
        }
    }

  @tailrec
  private[codec] def encodeSchema[A](
    value: A,
    name: Option[String],
    schema: Schema[A],
    writer: FieldWriter
  )(implicit alloc: BufferAllocator): Unit =
    schema match {
      case Schema.Primitive(standardType, _)       =>
        encodePrimitive(value, name, standardType, writer)
      case record: Schema.Record[A]                =>
        val writer0 = name.fold[FieldWriter](writer.struct().asInstanceOf[UnionListWriter])(
          writer.struct(_).asInstanceOf[PromotableWriter]
        )
        encodeCaseClass(value, record.fields, writer0)
      case Schema.Sequence(elemSchema, _, g, _, _) =>
        val writer0 = name.fold(writer.list)(writer.list).asInstanceOf[PromotableWriter]
        encodeSequence(g(value), elemSchema, writer0)
      case lzy: Schema.Lazy[_]                     =>
        encodeSchema(value, name, lzy.schema, writer)
      case other                                   =>
        throw EncoderError(s"Unsupported ZIO Schema type $other")
    }

  private[codec] def encodeCaseClass[A](
    value: A,
    fields: Chunk[Schema.Field[A, _]],
    writer: FieldWriter
  )(implicit alloc: BufferAllocator): Unit = {
    writer.start()
    fields.foreach { case Schema.Field(name, schema0, _, _, get, _) =>
      encodeSchema(get(value), Some(name), schema0.asInstanceOf[Schema[Any]], writer)
    }
    writer.end()
  }

  private[codec] def encodeSequence[A](
    chunk: Chunk[A],
    schema0: Schema[A],
    writer: FieldWriter
  )(implicit alloc: BufferAllocator): Unit = {
    val it = chunk.iterator

    writer.startList()
    it.foreach(encodeSchema(_, None, schema0, writer))
    writer.endList()
  }

  private[codec] def encodePrimitive[A](
    value: A,
    name: Option[String],
    standardType: StandardType[A],
    writer: FieldWriter
  )(implicit alloc: BufferAllocator): Unit = {

    def withBuffer(size: Long)(fn: ArrowBuf => Unit) = {
      val buffer = alloc.buffer(size)
      fn(buffer)
      buffer.close()
    }

    (standardType, value) match {
      case (StandardType.StringType, v: String)                   =>
        withBuffer(v.length.toLong) { buffer =>
          buffer.writeBytes(v.getBytes(StandardCharsets.UTF_8))
          name.fold(writer.varChar)(writer.varChar).writeVarChar(0, v.length, buffer)
        }
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
        withBuffer(16L) { buffer =>
          buffer.setLong(0, v.getMostSignificantBits)
          buffer.setLong(8, v.getLeastSignificantBits)
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
        withBuffer(8) { buffer =>
          buffer.writeInt(v.getMonthValue)
          buffer.writeInt(v.getDayOfMonth)
          name.fold(writer.bigInt)(writer.bigInt).writeBigInt(buffer.getLong(0))
        }
      case (StandardType.PeriodType, v: Period)                   =>
        withBuffer(12) { buffer =>
          buffer.writeInt(v.getYears)
          buffer.writeInt(v.getMonths)
          buffer.writeInt(v.getDays)
          name.fold(writer.varBinary)(writer.varBinary).writeVarBinary(0, 12, buffer)
        }
      case (other, _)                                             =>
        throw EncoderError(s"Unsupported ZIO Schema StandardType $other")
    }
  }

  private[codec] def encodePrimitive[A, V <: ValueVector](
    value: A,
    standardType: StandardType[A],
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
        val bb = ByteBuffer.wrap(Array.ofDim(16))
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
      case (other, _, _)                                                                =>
        throw EncoderError(s"Unsupported ZIO Schema StandardType $other")
    }

}
