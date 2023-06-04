package me.mnedokushev.zio.apache.arrow.core.codec

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.complex.{ ListVector, StructVector }
import org.apache.arrow.vector.complex.impl.{ NullableStructWriter, PromotableWriter, UnionListWriter }
import org.apache.arrow.vector.types.pojo.{ ArrowType, FieldType }
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.writer.FieldWriter
import zio.Chunk
import zio.schema._

import java.nio.charset.StandardCharsets
import scala.util.control.NonFatal

trait ArrowVectorEncoder[-Val, Vector <: ValueVector] extends ArrowEncoder[Val, Vector] {

  override def encode(chunk: Chunk[Val])(implicit alloc: BufferAllocator): Either[Throwable, Vector] =
    try
      Right(encodeUnsafe(chunk))
    catch {
      case NonFatal(ex) => Left(ArrowEncoderError("Error encoding vector", Some(ex)))
    }

  protected def encodeUnsafe(chunk: Chunk[Val])(implicit alloc: BufferAllocator): Vector

  protected def init(alloc: BufferAllocator): Vector

}

object ArrowVectorEncoder {

  def apply[Val, Vector <: ValueVector](implicit
    encoder: ArrowVectorEncoder[Val, Vector]
  ): ArrowVectorEncoder[Val, Vector] =
    encoder

  implicit val booleanEncoder: ArrowVectorEncoder[Boolean, BitVector]   =
    scalar(new BitVector("bitVector", _))(_.allocateNew)(vec => (i, v) => vec.set(i, if (v) 1 else 0))
  implicit val intEncoder: ArrowVectorEncoder[Int, IntVector]           =
    scalar(new IntVector("intVector", _))(_.allocateNew)(_.set)
  implicit val longEncoder: ArrowVectorEncoder[Long, BigIntVector]      =
    scalar(new BigIntVector("longVector", _))(_.allocateNew)(_.set)
  implicit val stringEncoder: ArrowVectorEncoder[String, VarCharVector] =
    scalar(new VarCharVector("stringVector", _))(_.allocateNew)(vec =>
      (i, v) => vec.set(i, v.getBytes(StandardCharsets.UTF_8))
    )

  // Define schema for primitive types using Schema.primitive to reuse for list[Val: Schema]
  implicit def listBooleanEncoder[Col[x] <: Iterable[x]]: ArrowVectorEncoder[Col[Boolean], ListVector] =
    list(writer => v => writer.writeBit(if (v) 1 else 0))
  implicit def listIntEncoder[Col[x] <: Iterable[x]]: ArrowVectorEncoder[Col[Int], ListVector]         =
    list(_.writeInt)
  implicit def listLongEncoder[Col[x] <: Iterable[x]]: ArrowVectorEncoder[Col[Long], ListVector]       =
    list(_.writeBigInt)

  // TODO: support .list and .struct writer (latter means we need to add Schema[Val])
  def list[Val, Col[x] <: Iterable[x]](
    writeVal: UnionListWriter => Val => Unit
  ): ArrowVectorEncoder[Col[Val], ListVector] =
    new ArrowVectorEncoder[Col[Val], ListVector] {
      override protected def encodeUnsafe(chunk: Chunk[Col[Val]])(implicit alloc: BufferAllocator): ListVector = {
        val vec    = init(alloc)
        val len0   = chunk.length
        val writer = vec.getWriter
        val it0    = chunk.iterator
        var i      = 0

        while (it0.hasNext) {
          val nested = it0.next()
          val len    = nested.size
          val it     = nested.iterator

          writer.startList()
          writer.setPosition(i)
          while (it.hasNext)
            writeVal(writer)(it.next())
          writer.setValueCount(len)
          writer.endList()

          i += 1
        }

        vec.setValueCount(len0)
        vec
      }

      override protected def init(alloc: BufferAllocator): ListVector =
        ListVector.empty("listVector", alloc)
    }

  def scalar[Val, Vector <: ValueVector](initVec: BufferAllocator => Vector)(allocNew: Vector => Int => Unit)(
    setVal: Vector => (Int, Val) => Unit
  ): ArrowVectorEncoder[Val, Vector] =
    new ArrowVectorEncoder[Val, Vector] { self =>
      override protected def encodeUnsafe(chunk: Chunk[Val])(implicit alloc: BufferAllocator): Vector = {
        val vec = init(alloc)

        if (chunk.nonEmpty) {
          val len = chunk.length
          val it  = chunk.iterator
          var i   = 0

          allocNew(vec)(len)
          while (it.hasNext) {
            setVal(vec)(i, it.next())
            i += 1
          }
          vec.setValueCount(len)
        }

        vec
      }

      override protected def init(alloc: BufferAllocator): Vector = {
        val vec = initVec(alloc)

        vec.setValueCount(0)
        vec
      }
    }

  def struct[Val](implicit schema: Schema[Val]): ArrowVectorEncoder[Val, StructVector] =
    new ArrowVectorEncoder[Val, StructVector] {
      override protected def encodeUnsafe(chunk: Chunk[Val])(implicit alloc: BufferAllocator): StructVector = {

        def encodeSchema[A](value: A, name: Option[String], schema0: Schema[A], writer0: FieldWriter): Unit = {
          def structWriter =
            name.fold[FieldWriter](
              writer0.struct().asInstanceOf[UnionListWriter]
            )(
              writer0.struct(_).asInstanceOf[PromotableWriter]
            )
          def listWriter   =
            name.fold(writer0.list)(writer0.list).asInstanceOf[PromotableWriter]

          schema0 match {
            case cc: Schema.CaseClass1[_, _]                                                                 =>
              encodeCaseClass(value, cc.fields, structWriter)
            case cc: Schema.CaseClass2[_, _, _]                                                              =>
              encodeCaseClass(value, cc.fields, structWriter)
            case cc: Schema.CaseClass3[_, _, _, _]                                                           =>
              encodeCaseClass(value, cc.fields, structWriter)
            case cc: Schema.CaseClass4[_, _, _, _, _]                                                        =>
              encodeCaseClass(value, cc.fields, structWriter)
            case cc: Schema.CaseClass5[_, _, _, _, _, _]                                                     =>
              encodeCaseClass(value, cc.fields, structWriter)
            case cc: Schema.CaseClass6[_, _, _, _, _, _, _]                                                  =>
              encodeCaseClass(value, cc.fields, structWriter)
            case cc: Schema.CaseClass7[_, _, _, _, _, _, _, _]                                               =>
              encodeCaseClass(value, cc.fields, structWriter)
            case cc: Schema.CaseClass8[_, _, _, _, _, _, _, _, _]                                            =>
              encodeCaseClass(value, cc.fields, structWriter)
            case cc: Schema.CaseClass9[_, _, _, _, _, _, _, _, _, _]                                         =>
              encodeCaseClass(value, cc.fields, structWriter)
            case cc: Schema.CaseClass10[_, _, _, _, _, _, _, _, _, _, _]                                     =>
              encodeCaseClass(value, cc.fields, structWriter)
            case cc: Schema.CaseClass11[_, _, _, _, _, _, _, _, _, _, _, _]                                  =>
              encodeCaseClass(value, cc.fields, structWriter)
            case cc: Schema.CaseClass12[_, _, _, _, _, _, _, _, _, _, _, _, _]                               =>
              encodeCaseClass(value, cc.fields, structWriter)
            case cc: Schema.CaseClass13[_, _, _, _, _, _, _, _, _, _, _, _, _, _]                            =>
              encodeCaseClass(value, cc.fields, structWriter)
            case cc: Schema.CaseClass14[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _]                         =>
              encodeCaseClass(value, cc.fields, structWriter)
            case cc: Schema.CaseClass15[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]                      =>
              encodeCaseClass(value, cc.fields, structWriter)
            case cc: Schema.CaseClass16[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]                   =>
              encodeCaseClass(value, cc.fields, structWriter)
            case cc: Schema.CaseClass17[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]                =>
              encodeCaseClass(value, cc.fields, structWriter)
            case cc: Schema.CaseClass18[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]             =>
              encodeCaseClass(value, cc.fields, structWriter)
            case cc: Schema.CaseClass19[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]          =>
              encodeCaseClass(value, cc.fields, structWriter)
            case cc: Schema.CaseClass20[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]       =>
              encodeCaseClass(value, cc.fields, structWriter)
            case cc: Schema.CaseClass21[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]    =>
              encodeCaseClass(value, cc.fields, structWriter)
            case cc: Schema.CaseClass22[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _] =>
              encodeCaseClass(value, cc.fields, structWriter)
            case Schema.Primitive(standardType, _)                                                           =>
              encodePrimitive(value, name, standardType, writer0)
            case Schema.Sequence(elemSchema, _, g, _, _)                                                     =>
              encodeSequence(g(value), elemSchema, listWriter)
            case lzy: Schema.Lazy[_]                                                                         =>
              encodeSchema(value, name, lzy.schema, writer0)
            case other                                                                                       =>
              throw ArrowEncoderError(s"Unsupported ZIO Schema type $other")

          }
        }

        def encodeSequence[A](chunk: Chunk[A], schema0: Schema[A], writer0: FieldWriter): Unit = {
          val it = chunk.iterator

          writer0.startList()
          while (it.hasNext)
            encodeSchema(it.next(), None, schema0, writer0)
          writer0.endList()
        }

        def encodePrimitive[A](
          value: A,
          name: Option[String],
          standardType: StandardType[A],
          writer0: FieldWriter
        ): Unit =
          (standardType, value) match {
            case (StandardType.StringType, s: String) =>
              val buffer = alloc.buffer(s.length)
              buffer.writeBytes(s.getBytes(StandardCharsets.UTF_8))
              name.fold(writer0.varChar)(writer0.varChar).writeVarChar(0, s.length, buffer)
            case (StandardType.BoolType, b: Boolean)  =>
              name.fold(writer0.bit)(writer0.bit).writeBit(if (b) 1 else 0)
            case (StandardType.IntType, i: Int)       =>
              name.fold(writer0.integer)(writer0.integer).writeInt(i)
            case (StandardType.LongType, l: Long)     =>
              name.fold(writer0.bigInt)(writer0.bigInt).writeBigInt(l)
            case (StandardType.FloatType, f: Float)   =>
              name.fold(writer0.float4)(writer0.float4).writeFloat4(f)
            case (StandardType.DoubleType, d: Double) =>
              name.fold(writer0.float8)(writer0.float8).writeFloat8(d)
            case (other, _)                           =>
              throw ArrowEncoderError(s"Unsupported ZIO Schema StandardType $other")
          }

        def encodeCaseClass[A](value: A, fields: Chunk[Schema.Field[A, _]], writer0: FieldWriter): Unit = {
          writer0.start()
          fields.foreach { case Schema.Field(name, schema0, _, _, get, _) =>
            encodeSchema(get(value), Some(name), schema0.asInstanceOf[Schema[Any]], writer0)
          }
          writer0.end()
        }

        schema match {
          case record: Schema.Record[Val] =>
            val vec    = init(alloc)
            val len    = chunk.length
            val writer = vec.getWriter
            val it     = chunk.iterator
            var i      = 0

            while (it.hasNext) {
              writer.setPosition(i)
              encodeCaseClass(it.next(), record.fields, writer)
              vec.setIndexDefined(i)
              i += 1
            }
            writer.setValueCount(len)

            vec
          case _                          =>
            throw ArrowEncoderError(s"Given ZIO schema must be of type Schema.Record[Val]")
        }

      }

      override protected def init(alloc: BufferAllocator): StructVector =
        StructVector.empty("structVector", alloc)

    }

}
