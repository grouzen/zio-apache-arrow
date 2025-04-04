package me.mnedokushev.zio.apache.arrow.core.codec

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.complex.impl.{ PromotableWriter, UnionListWriter }
import org.apache.arrow.vector.complex.writer.FieldWriter
import org.apache.arrow.vector.complex.{ ListVector, StructVector }
import org.apache.arrow.vector.{ ValueVector, _ }
import zio.Chunk
import zio.schema.{ Deriver, Schema, StandardType }

object ValueVectorEncoderDeriver {

  def default[V1 <: ValueVector]: Deriver[ValueVectorEncoder[V1, *]] = new Deriver[ValueVectorEncoder[V1, *]] {

    override def deriveRecord[A](
      record: Schema.Record[A],
      fields: => Chunk[Deriver.WrappedF[ValueVectorEncoder[V1, *], ?]],
      summoned: => Option[ValueVectorEncoder[V1, A]]
    ): ValueVectorEncoder[V1, A] = new ValueVectorEncoder[V1, A] {

      private val encoders = fields.map(_.unwrap)

      override def encodeUnsafe(chunk: Chunk[Option[A]], nullable: Boolean)(implicit alloc: BufferAllocator): V1 = {
        val vec    = StructVector.empty("structVector", alloc)
        val writer = vec.getWriter
        val len    = chunk.length
        val it     = chunk.iterator.zipWithIndex

        it.foreach { case (v, i) =>
          writer.setPosition(i)

          if (nullable && v.isEmpty)
            writer.writeNull()
          else
            ValueEncoder.encodeStruct(v.get, record.fields, encoders, writer)

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
      cases: => Chunk[Deriver.WrappedF[ValueVectorEncoder[V1, *], ?]],
      summoned: => Option[ValueVectorEncoder[V1, A]]
    ): ValueVectorEncoder[V1, A] = ???

    override def derivePrimitive[A](
      st: StandardType[A],
      summoned: => Option[ValueVectorEncoder[V1, A]]
    ): ValueVectorEncoder[V1, A] =
      ValueVectorEncoder.primitive[V1, A](
        allocateVec = { alloc =>
          val vec = st match {
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
          vec.asInstanceOf[V1]
        },
        getWriter = vec => primitiveWriter(st, vec.asInstanceOf[FieldVector]),
        encodeTopLevel = (v, writer, alloc) => ValueEncoder.encodePrimitive(st, v, writer)(alloc),
        encodeNested = (v, name, writer, alloc) => ValueEncoder.encodePrimitive(st, v, name, writer)(alloc)
      )(st)

    override def deriveOption[A](
      option: Schema.Optional[A],
      inner: => ValueVectorEncoder[V1, A],
      summoned: => Option[ValueVectorEncoder[V1, Option[A]]]
    ): ValueVectorEncoder[V1, Option[A]] = new ValueVectorEncoder[V1, Option[A]] {

      override def encodeUnsafe(chunk: Chunk[Option[Option[A]]], nullable: Boolean)(implicit
        alloc: BufferAllocator
      ): V1 =
        inner.encodeUnsafe(chunk.map(_.get), nullable = true)

      override def encodeValue(value: Option[A], name: Option[String], writer: FieldWriter)(implicit
        alloc: BufferAllocator
      ): Unit =
        value match {
          case Some(value0) =>
            inner.encodeValue(value0, name, writer)
          case None         =>
            writer.writeNull()
        }

    }

    override def deriveSequence[C[_], A](
      sequence: Schema.Sequence[C[A], A, ?],
      inner: => ValueVectorEncoder[V1, A],
      summoned: => Option[ValueVectorEncoder[V1, C[A]]]
    ): ValueVectorEncoder[V1, C[A]] =
      new ValueVectorEncoder[V1, C[A]] {

        override def encodeUnsafe(chunk: Chunk[Option[C[A]]], nullable: Boolean)(implicit
          alloc: BufferAllocator
        ): V1 = {
          val vec    = ListVector.empty("listVector", alloc)
          val writer = vec.getWriter
          val len    = chunk.length
          val it     = chunk.iterator.zipWithIndex

          it.foreach { case (vs, i) =>
            writer.setPosition(i)

            if (nullable && vs.isEmpty)
              writer.writeNull()
            else
              ValueEncoder.encodeList(sequence.toChunk(vs.get), inner, writer)
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
      transform: Schema.Transform[A, B, ?],
      fields: => Chunk[Deriver.WrappedF[ValueVectorEncoder[V1, *], ?]],
      summoned: => Option[ValueVectorEncoder[V1, B]]
    ): ValueVectorEncoder[V1, B] = ???

  }.cached

  def summoned[V1 <: ValueVector]: Deriver[ValueVectorEncoder[V1, *]] =
    default.autoAcceptSummoned

}
