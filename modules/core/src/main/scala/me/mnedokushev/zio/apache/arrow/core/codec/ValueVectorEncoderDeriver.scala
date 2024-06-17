package me.mnedokushev.zio.apache.arrow.core.codec

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.complex.impl.{ PromotableWriter, UnionListWriter }
import org.apache.arrow.vector.complex.writer.FieldWriter
import org.apache.arrow.vector.complex.{ ListVector, StructVector }
import org.apache.arrow.vector.{ ValueVector, _ }
import zio.Chunk
import zio.schema.{ Deriver, Schema, StandardType }
import org.apache.arrow.vector.complex.impl._

object ValueVectorEncoderDeriver {

  def default[V1 <: ValueVector]: Deriver[ValueVectorEncoder[V1, *]] = new Deriver[ValueVectorEncoder[V1, *]] {

    override def deriveRecord[A](
      record: Schema.Record[A],
      fields: => Chunk[Deriver.WrappedF[ValueVectorEncoder[V1, *], _]],
      summoned: => Option[ValueVectorEncoder[V1, A]]
    ): ValueVectorEncoder[V1, A] = new ValueVectorEncoder[V1, A] {

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

      // override def allocateVector(implicit alloc: BufferAllocator): V1 =
      //   StructVector.empty("structVector", alloc).asInstanceOf[V1]

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
      cases: => Chunk[Deriver.WrappedF[ValueVectorEncoder[V1, *], _]],
      summoned: => Option[ValueVectorEncoder[V1, A]]
    ): ValueVectorEncoder[V1, A] = ???

    override def derivePrimitive[A](
      st: StandardType[A],
      summoned: => Option[ValueVectorEncoder[V1, A]]
    ): ValueVectorEncoder[V1, A] = new ValueVectorEncoder[V1, A] {

      override protected def encodeUnsafe(chunk: Chunk[A])(implicit alloc: BufferAllocator): V1 = {
        val vec                 = st match {
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
        val writer: FieldWriter = (st, vec) match {
          case (StandardType.StringType, vec0: VarCharVector)         =>
            new VarCharWriterImpl(vec0)
          case (StandardType.BoolType, vec0: BitVector)               =>
            new BitWriterImpl(vec0)
          case (StandardType.ByteType, vec0: UInt1Vector)             =>
            new UInt1WriterImpl(vec0)
          case (StandardType.ShortType, vec0: SmallIntVector)         =>
            new SmallIntWriterImpl(vec0)
          case (StandardType.IntType, vec0: IntVector)                =>
            new IntWriterImpl(vec0)
          case (StandardType.LongType, vec0: BigIntVector)            =>
            new BigIntWriterImpl(vec0)
          case (StandardType.FloatType, vec0: Float4Vector)           =>
            new Float4WriterImpl(vec0)
          case (StandardType.DoubleType, vec0: Float8Vector)          =>
            new Float8WriterImpl(vec0)
          case (StandardType.BinaryType, vec0: LargeVarBinaryVector)  =>
            new LargeVarBinaryWriterImpl(vec0)
          case (StandardType.CharType, vec0: UInt2Vector)             =>
            new UInt2WriterImpl(vec0)
          case (StandardType.UUIDType, vec0: VarBinaryVector)         =>
            new VarBinaryWriterImpl(vec0)
          case (StandardType.BigDecimalType, vec0: DecimalVector)     =>
            new DecimalWriterImpl(vec0)
          case (StandardType.BigIntegerType, vec0: VarBinaryVector)   =>
            new VarBinaryWriterImpl(vec0)
          case (StandardType.DayOfWeekType, vec0: IntVector)          =>
            new IntWriterImpl(vec0)
          case (StandardType.MonthType, vec0: IntVector)              =>
            new IntWriterImpl(vec0)
          case (StandardType.MonthDayType, vec0: BigIntVector)        =>
            new BigIntWriterImpl(vec0)
          case (StandardType.PeriodType, vec0: VarBinaryVector)       =>
            new VarBinaryWriterImpl(vec0)
          case (StandardType.YearType, vec0: IntVector)               =>
            new IntWriterImpl(vec0)
          case (StandardType.YearMonthType, vec0: BigIntVector)       =>
            new BigIntWriterImpl(vec0)
          case (StandardType.ZoneIdType, vec0: VarCharVector)         =>
            new VarCharWriterImpl(vec0)
          case (StandardType.ZoneOffsetType, vec0: VarCharVector)     =>
            new VarCharWriterImpl(vec0)
          case (StandardType.DurationType, vec0: BigIntVector)        =>
            new BigIntWriterImpl(vec0)
          case (StandardType.InstantType, vec0: BigIntVector)         =>
            new BigIntWriterImpl(vec0)
          case (StandardType.LocalDateType, vec0: VarCharVector)      =>
            new VarCharWriterImpl(vec0)
          case (StandardType.LocalTimeType, vec0: VarCharVector)      =>
            new VarCharWriterImpl(vec0)
          case (StandardType.LocalDateTimeType, vec0: VarCharVector)  =>
            new VarCharWriterImpl(vec0)
          case (StandardType.OffsetTimeType, vec0: VarCharVector)     =>
            new VarCharWriterImpl(vec0)
          case (StandardType.OffsetDateTimeType, vec0: VarCharVector) =>
            new VarCharWriterImpl(vec0)
          case (StandardType.ZonedDateTimeType, vec0: VarCharVector)  =>
            new VarCharWriterImpl(vec0)
          case _                                                      =>
            throw EncoderError("Unsupported ZIO Schema StandardType")
        }
        val len                 = chunk.length
        val it                  = chunk.iterator.zipWithIndex

        it.foreach { case (v, i) =>
          writer.setPosition(i)
          ValueEncoder.encodePrimitive(st, v, writer)
        }

        vec.setValueCount(len)
        vec.asInstanceOf[V1]
      }

      override def encodeValue(value: A, name: Option[String], writer: FieldWriter)(implicit
        alloc: BufferAllocator
      ): Unit = ValueEncoder.encodePrimitive(st, value, name, writer)

    }

    override def deriveOption[A](
      option: Schema.Optional[A],
      inner: => ValueVectorEncoder[V1, A],
      summoned: => Option[ValueVectorEncoder[V1, Option[A]]]
    ): ValueVectorEncoder[V1, Option[A]] = ???

    // override def deriveOption[A](
    //   option: Schema.Optional[A],
    //   inner: => ValueVectorEncoder[V1, A],
    //   summoned: => Option[ValueVectorEncoder[V1, Option[A]]]
    // ): ValueVectorEncoder[V1, Option[A]] = new ValueVectorEncoder[V1, Option[A]] {

    //   override protected def encodeUnsafe(chunk: Chunk[Option[A]], vector: V1)(implicit alloc: BufferAllocator): V1 = {
    //     val writer = vector.getWriter

    //     writer
    //   }

    //   override def allocateVector(implicit alloc: BufferAllocator): V1 =
    //     inner.allocateVector

    //   override def encodeValue(value: Option[A], name: Option[String], writer: FieldWriter)(implicit
    //     alloc: BufferAllocator
    //   ): Unit = ???

    // }

    override def deriveSequence[C[_], A](
      sequence: Schema.Sequence[C[A], A, _],
      inner: => ValueVectorEncoder[V1, A],
      summoned: => Option[ValueVectorEncoder[V1, C[A]]]
    ): ValueVectorEncoder[V1, C[A]] =
      new ValueVectorEncoder[V1, C[A]] {

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
      key: => ValueVectorEncoder[V1, K],
      value: => ValueVectorEncoder[V1, V],
      summoned: => Option[ValueVectorEncoder[V1, Map[K, V]]]
    ): ValueVectorEncoder[V1, Map[K, V]] = ???

    override def deriveTransformedRecord[A, B](
      record: Schema.Record[A],
      transform: Schema.Transform[A, B, _],
      fields: => Chunk[Deriver.WrappedF[ValueVectorEncoder[V1, *], _]],
      summoned: => Option[ValueVectorEncoder[V1, B]]
    ): ValueVectorEncoder[V1, B] = ???

  }.cached

  def summoned[V1 <: ValueVector]: Deriver[ValueVectorEncoder[V1, *]] =
    default.autoAcceptSummoned

}
