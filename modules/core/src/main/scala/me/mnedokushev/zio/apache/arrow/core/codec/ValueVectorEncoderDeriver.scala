package me.mnedokushev.zio.apache.arrow.core.codec

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.complex.impl.{ PromotableWriter, UnionListWriter }
import org.apache.arrow.vector.complex.writer.FieldWriter
import org.apache.arrow.vector.complex.{ ListVector, StructVector }
import org.apache.arrow.vector.{ ValueVector, _ }
import zio.Chunk
import zio.schema.{ Deriver, Schema, StandardType }

object ValueVectorEncoderDeriver {

  def default[V1 <: ValueVector]: Deriver[ValueVectorEncoder[*, V1]] = new Deriver[ValueVectorEncoder[*, V1]] {

    override def deriveRecord[A](
      record: Schema.Record[A],
      fields: => Chunk[Deriver.WrappedF[ValueVectorEncoder[*, V1], _]],
      summoned: => Option[ValueVectorEncoder[A, V1]]
    ): ValueVectorEncoder[A, V1] = new ValueVectorEncoder[A, V1] {

      private val encoders = fields.map(_.unwrap)

      override protected def encodeUnsafe(chunk: Chunk[A])(implicit alloc: BufferAllocator): V1 = {
        val vec    = StructVector.empty("structVector", alloc)
        val len    = chunk.length
        val writer = vec.getWriter
        val it     = chunk.iterator.zipWithIndex

        it.foreach { case (v, i) =>
          writer.setPosition(i)
          ValueEncoder.encodeStruct(v, record.fields, encoders, writer)
          vec.setIndexDefined(i)
        }
        writer.setValueCount(len)

        vec.asInstanceOf[V1]
      }

      override def encodeValue(value: A, name: Option[String], writer: FieldWriter)(implicit
        alloc: BufferAllocator
      ): Unit = {
        val writer0 = name.fold[FieldWriter](writer.struct().asInstanceOf[UnionListWriter])(
          writer.struct(_).asInstanceOf[PromotableWriter]
        )

        ValueEncoder.encodeStruct(value, record.fields, encoders, writer0)

      }

    }

    override def deriveEnum[A](
      `enum`: Schema.Enum[A],
      cases: => Chunk[Deriver.WrappedF[ValueVectorEncoder[*, V1], _]],
      summoned: => Option[ValueVectorEncoder[A, V1]]
    ): ValueVectorEncoder[A, V1] = ???

    override def derivePrimitive[A](
      st: StandardType[A],
      summoned: => Option[ValueVectorEncoder[A, V1]]
    ): ValueVectorEncoder[A, V1] = new ValueVectorEncoder[A, V1] {

      override protected def encodeUnsafe(chunk: Chunk[A])(implicit alloc: BufferAllocator): V1 = {
        def allocate[A1](standardType: StandardType[A1]): V1 = {
          val vec = standardType match {
            case StandardType.StringType         =>
              new VarCharVector("stringVector", alloc)
            case StandardType.BoolType           =>
              new BitVector("boolVector", alloc)
            case StandardType.ByteType           =>
              new UInt1Vector("byteVector", alloc)
            case StandardType.ShortType          =>
              new SmallIntVector("shortVector", alloc)
            case StandardType.IntType            =>
              new IntVector("intVector", alloc)
            case StandardType.LongType           =>
              new BigIntVector("longVector", alloc)
            case StandardType.FloatType          =>
              new Float4Vector("floatVector", alloc)
            case StandardType.DoubleType         =>
              new Float8Vector("doubleVector", alloc)
            case StandardType.BinaryType         =>
              new LargeVarBinaryVector("binaryVector", alloc)
            case StandardType.CharType           =>
              new UInt2Vector("charVector", alloc)
            case StandardType.UUIDType           =>
              new VarBinaryVector("uuidVector", alloc)
            case StandardType.BigDecimalType     =>
              new DecimalVector("bigDecimalVector", alloc, 11, 2)
            case StandardType.BigIntegerType     =>
              new VarBinaryVector("bigIntVector", alloc)
            case StandardType.DayOfWeekType      =>
              new IntVector("dayOfWeekVector", alloc)
            case StandardType.MonthType          =>
              new IntVector("monthVector", alloc)
            case StandardType.MonthDayType       =>
              new BigIntVector("monthDayVector", alloc)
            case StandardType.PeriodType         =>
              new VarBinaryVector("periodVector", alloc)
            case StandardType.YearType           =>
              new IntVector("yearVector", alloc)
            case StandardType.YearMonthType      =>
              new BigIntVector("yearMonthVector", alloc)
            case StandardType.ZoneIdType         =>
              new VarCharVector("zoneIdVector", alloc)
            case StandardType.ZoneOffsetType     =>
              new VarCharVector("zoneOffsetVector", alloc)
            case StandardType.DurationType       =>
              new BigIntVector("durationVector", alloc)
            case StandardType.InstantType        =>
              new BigIntVector("instantVector", alloc)
            case StandardType.LocalDateType      =>
              new VarCharVector("localDateVector", alloc)
            case StandardType.LocalTimeType      =>
              new VarCharVector("localTimeVector", alloc)
            case StandardType.LocalDateTimeType  =>
              new VarCharVector("localDateTimeVector", alloc)
            case StandardType.OffsetTimeType     =>
              new VarCharVector("offsetTimeVector", alloc)
            case StandardType.OffsetDateTimeType =>
              new VarCharVector("offsetDateTimeVector", alloc)
            case StandardType.ZonedDateTimeType  =>
              new VarCharVector("zoneDateTimeVector", alloc)
            case other                           =>
              throw EncoderError(s"Unsupported ZIO Schema StandardType $other")
          }

          vec.allocateNew()
          vec.asInstanceOf[V1]
        }

        val vec = allocate(st)
        val len = chunk.length
        val it  = chunk.iterator.zipWithIndex

        it.foreach { case (v, i) =>
          ValueEncoder.encodePrimitive(st, v, vec, i)
        }

        vec.setValueCount(len)
        vec
      }

      override def encodeValue(value: A, name: Option[String], writer: FieldWriter)(implicit
        alloc: BufferAllocator
      ): Unit = ValueEncoder.encodePrimitive(st, value, name, writer)

    }

    override def deriveOption[A](
      option: Schema.Optional[A],
      inner: => ValueVectorEncoder[A, V1],
      summoned: => Option[ValueVectorEncoder[Option[A], V1]]
    ): ValueVectorEncoder[Option[A], V1] = ???

    override def deriveSequence[C[_], A](
      sequence: Schema.Sequence[C[A], A, _],
      inner: => ValueVectorEncoder[A, V1],
      summoned: => Option[ValueVectorEncoder[C[A], V1]]
    ): ValueVectorEncoder[C[A], V1] =
      new ValueVectorEncoder[C[A], V1] {

        override protected def encodeUnsafe(chunk: Chunk[C[A]])(implicit alloc: BufferAllocator): V1 = {
          val vec    = ListVector.empty("listVector", alloc)
          val len    = chunk.length
          val writer = vec.getWriter
          val it     = chunk.iterator

          it.foreach { vs =>
            writer.startList()
            sequence.toChunk(vs).foreach(inner.encodeValue(_, None, writer))
            writer.endList()
          }

          vec.setValueCount(len)
          vec.asInstanceOf[V1]
        }

        override def encodeValue(value: C[A], name: Option[String], writer: FieldWriter)(implicit
          alloc: BufferAllocator
        ): Unit = {
          val writer0 = name.fold(writer.list)(writer.list).asInstanceOf[PromotableWriter]

          ValueEncoder.encodeList(sequence.toChunk(value), inner, writer0)
        }

      }

    override def deriveMap[K, V](
      map: Schema.Map[K, V],
      key: => ValueVectorEncoder[K, V1],
      value: => ValueVectorEncoder[V, V1],
      summoned: => Option[ValueVectorEncoder[Map[K, V], V1]]
    ): ValueVectorEncoder[Map[K, V], V1] = ???

    override def deriveTransformedRecord[A, B](
      record: Schema.Record[A],
      transform: Schema.Transform[A, B, _],
      fields: => Chunk[Deriver.WrappedF[ValueVectorEncoder[*, V1], _]],
      summoned: => Option[ValueVectorEncoder[B, V1]]
    ): ValueVectorEncoder[B, V1] = ???

  }

}
