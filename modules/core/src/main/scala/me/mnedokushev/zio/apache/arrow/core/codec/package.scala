package me.mnedokushev.zio.apache.arrow.core

import zio.schema.StandardType
import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.complex.writer.FieldWriter
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.impl._

package object codec {

  def primitiveWriter(st: StandardType[_], vec: FieldVector): FieldWriter =
    (st, vec) match {
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
      case (other, _)                                             =>
        throw EncoderError(s"Unsupported ZIO Schema StandardType $other")
    }

}
