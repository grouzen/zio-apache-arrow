package me.mnedokushev.zio.apache.arrow.core.codec

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.complex.{ ListVector, StructVector }
import org.apache.arrow.vector.complex.impl.{ PromotableWriter, UnionListWriter }
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.writer.FieldWriter
import zio.Chunk
import zio.schema._

import java.nio.charset.StandardCharsets
import scala.annotation.tailrec
import scala.util.control.NonFatal

trait ArrowVectorEncoder[-Val, Vector <: ValueVector] extends ArrowEncoder[Val, Vector] {

  override def encode(chunk: Chunk[Val])(implicit alloc: BufferAllocator): Either[Throwable, Vector] =
    try
      Right(encodeUnsafe(chunk))
    catch {
      case NonFatal(ex) => Left(ArrowEncoderError("Error encoding vector", Some(ex)))
    }

  protected def encodeUnsafe(chunk: Chunk[Val])(implicit alloc: BufferAllocator): Vector

}

object ArrowVectorEncoder {

  def apply[Val, Vector <: ValueVector](implicit
    encoder: ArrowVectorEncoder[Val, Vector]
  ): ArrowVectorEncoder[Val, Vector] =
    encoder

  implicit val booleanEncoder: ArrowVectorEncoder[Boolean, BitVector]   =
    primitive(new BitVector("bitVector", _))(_.allocateNew)(vec => (i, v) => vec.set(i, if (v) 1 else 0))
  implicit val intEncoder: ArrowVectorEncoder[Int, IntVector]           =
    primitive(new IntVector("intVector", _))(_.allocateNew)(_.set)
  implicit val longEncoder: ArrowVectorEncoder[Long, BigIntVector]      =
    primitive(new BigIntVector("longVector", _))(_.allocateNew)(_.set)
  implicit val stringEncoder: ArrowVectorEncoder[String, VarCharVector] =
    primitive(new VarCharVector("stringVector", _))(_.allocateNew)(vec =>
      (i, v) => vec.set(i, v.getBytes(StandardCharsets.UTF_8))
    )

  def primitive[Val, Vector <: ValueVector](initVec: BufferAllocator => Vector)(allocNew: Vector => Int => Unit)(
    setVal: Vector => (Int, Val) => Unit
  ): ArrowVectorEncoder[Val, Vector] =
    new ArrowVectorEncoder[Val, Vector] {
      override protected def encodeUnsafe(chunk: Chunk[Val])(implicit alloc: BufferAllocator): Vector = {
        val vec = initVec(alloc)
        val len = chunk.length

        if (chunk.nonEmpty) {
          val it = chunk.iterator.zipWithIndex

          allocNew(vec)(len)
          it.foreach { case (v, i) =>
            setVal(vec)(i, v)
          }
        }

        vec.setValueCount(len)
        vec
      }
    }

  implicit def list[Val, Col[x] <: Iterable[x]](implicit
    schema: Schema[Val]
  ): ArrowVectorEncoder[Col[Val], ListVector] =
    new ArrowVectorEncoder[Col[Val], ListVector] {
      override protected def encodeUnsafe(chunk: Chunk[Col[Val]])(implicit alloc: BufferAllocator): ListVector = {
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

  implicit def struct[Val](implicit schema: Schema[Val]): ArrowVectorEncoder[Val, StructVector] =
    new ArrowVectorEncoder[Val, StructVector] {
      override protected def encodeUnsafe(chunk: Chunk[Val])(implicit alloc: BufferAllocator): StructVector =
        schema match {
          case record: Schema.Record[Val] =>
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
          case _                          =>
            throw ArrowEncoderError(s"Given ZIO schema must be of type Schema.Record[Val]")
        }
    }

  private def encodeCaseClass[A](value: A, fields: Chunk[Schema.Field[A, _]], writer0: FieldWriter)(implicit
    alloc: BufferAllocator
  ): Unit = {
    writer0.start()
    fields.foreach { case Schema.Field(name, schema0, _, _, get, _) =>
      encodeSchema(get(value), Some(name), schema0.asInstanceOf[Schema[Any]], writer0)
    }
    writer0.end()
  }

  @tailrec
  private def encodeSchema[A](value: A, name: Option[String], schema0: Schema[A], writer0: FieldWriter)(implicit
    alloc: BufferAllocator
  ): Unit =
    schema0 match {
      case Schema.Primitive(standardType, _)       =>
        encodePrimitive(value, name, standardType, writer0)
      case record: Schema.Record[A]                =>
        val writer = name.fold[FieldWriter](writer0.struct().asInstanceOf[UnionListWriter])(
          writer0.struct(_).asInstanceOf[PromotableWriter]
        )
        encodeCaseClass(value, record.fields, writer)
      case Schema.Sequence(elemSchema, _, g, _, _) =>
        val writer = name.fold(writer0.list)(writer0.list).asInstanceOf[PromotableWriter]
        encodeSequence(g(value), elemSchema, writer)
      case lzy: Schema.Lazy[_]                     =>
        encodeSchema(value, name, lzy.schema, writer0)
      case other                                   =>
        throw ArrowEncoderError(s"Unsupported ZIO Schema type $other")
    }

  private def encodeSequence[A](chunk: Chunk[A], schema0: Schema[A], writer0: FieldWriter)(implicit
    alloc: BufferAllocator
  ): Unit = {
    val it = chunk.iterator

    writer0.startList()
    it.foreach(encodeSchema(_, None, schema0, writer0))
    writer0.endList()
  }

  private def encodePrimitive[A](
    value: A,
    name: Option[String],
    standardType: StandardType[A],
    writer0: FieldWriter
  )(implicit alloc: BufferAllocator): Unit =
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

}
