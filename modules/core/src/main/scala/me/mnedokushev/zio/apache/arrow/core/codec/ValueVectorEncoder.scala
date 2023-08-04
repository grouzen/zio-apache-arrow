package me.mnedokushev.zio.apache.arrow.core.codec

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.complex.{ ListVector, StructVector }
import org.apache.arrow.vector.complex.impl.{ PromotableWriter, UnionListWriter }
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.writer.FieldWriter
import zio._
import zio.schema._

import java.nio.charset.StandardCharsets
import scala.annotation.tailrec
import scala.util.control.NonFatal

trait ValueVectorEncoder[-Val, Vector <: ValueVector] {

  final def encodeZIO(chunk: Chunk[Val]): RIO[Scope with BufferAllocator, Vector] =
    ZIO.fromAutoCloseable(
      ZIO.serviceWithZIO[BufferAllocator] { implicit alloc =>
        ZIO.fromEither(encode(chunk))
      }
    )

  final def encode(chunk: Chunk[Val])(implicit alloc: BufferAllocator): Either[Throwable, Vector] =
    try
      Right(encodeUnsafe(chunk))
    catch {
      case encoderError: EncoderError => Left(encoderError)
      case NonFatal(ex)               => Left(EncoderError("Error encoding vector", Some(ex)))

    }

  protected def encodeUnsafe(chunk: Chunk[Val])(implicit alloc: BufferAllocator): Vector

}

object ValueVectorEncoder {

  def apply[Val, Vector <: ValueVector](implicit
    encoder: ValueVectorEncoder[Val, Vector]
  ): ValueVectorEncoder[Val, Vector] =
    encoder

  implicit def primitive[Val, Vector <: ValueVector](implicit
    schema: Schema[Val]
  ): ValueVectorEncoder[Val, Vector] =
    new ValueVectorEncoder[Val, Vector] {
      override protected def encodeUnsafe(chunk: Chunk[Val])(implicit alloc: BufferAllocator): Vector = {
        def allocate(standardType: StandardType[Val]): Vector = {
          val vec = standardType match {
            case StandardType.BoolType   =>
              new BitVector("bitVector", alloc)
            case StandardType.IntType    =>
              new IntVector("intVector", alloc)
            case StandardType.LongType   =>
              new BigIntVector("longVector", alloc)
            case StandardType.StringType =>
              new VarCharVector("stringVector", alloc)
            case other                   =>
              throw EncoderError(s"Unsupported ZIO Schema StandardType $other")
          }

          vec.allocateNew()
          vec.asInstanceOf[Vector]
        }

        schema match {
          case Schema.Primitive(standardType, _) =>
            val vec0 = allocate(standardType)
            val len  = chunk.length
            val it   = chunk.iterator.zipWithIndex

            it.foreach { case (v, i) =>
              encodePrimitive(v, standardType, vec0, i)
            }

            vec0.setValueCount(len)
            vec0
          case _                                 =>
            throw EncoderError(s"Given ZIO schema must be of type Schema.Primitive[Val]")

        }
      }
    }

  implicit def list[Val, Col[x] <: Iterable[x]](implicit
    schema: Schema[Val]
  ): ValueVectorEncoder[Col[Val], ListVector] =
    new ValueVectorEncoder[Col[Val], ListVector] {
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

  implicit def struct[Val](implicit schema: Schema[Val]): ValueVectorEncoder[Val, StructVector] =
    new ValueVectorEncoder[Val, StructVector] {
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
            throw EncoderError(s"Given ZIO schema must be of type Schema.Record[Val]")
        }
    }

  @tailrec
  private[codec] def encodeSchema[A](
    value: A,
    name: Option[String],
    schema0: Schema[A],
    writer0: FieldWriter
  )(implicit alloc: BufferAllocator): Unit =
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
        throw EncoderError(s"Unsupported ZIO Schema type $other")
    }

  private[codec] def encodeCaseClass[A](
    value: A,
    fields: Chunk[Schema.Field[A, _]],
    writer0: FieldWriter
  )(implicit alloc: BufferAllocator): Unit = {
    writer0.start()
    fields.foreach { case Schema.Field(name, schema0, _, _, get, _) =>
      encodeSchema(get(value), Some(name), schema0.asInstanceOf[Schema[Any]], writer0)
    }
    writer0.end()
  }

  private[codec] def encodeSequence[A](
    chunk: Chunk[A],
    schema0: Schema[A],
    writer0: FieldWriter
  )(implicit alloc: BufferAllocator): Unit = {
    val it = chunk.iterator

    writer0.startList()
    it.foreach(encodeSchema(_, None, schema0, writer0))
    writer0.endList()
  }

  private[codec] def encodePrimitive[A](
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
        buffer.close()
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
        throw EncoderError(s"Unsupported ZIO Schema StandardType $other")
    }

  private[codec] def encodePrimitive[A, V <: ValueVector](
    value: A,
    standardType: StandardType[A],
    vec: V,
    idx: Int
  ): Unit =
    (standardType, vec, value) match {
      case (StandardType.StringType, vec: VarCharVector, s: String) =>
        vec.set(idx, s.getBytes(StandardCharsets.UTF_8))
      case (StandardType.BoolType, vec: BitVector, b: Boolean)      =>
        vec.set(idx, if (b) 1 else 0)
      case (StandardType.IntType, vec: IntVector, ii: Int)          =>
        vec.set(idx, ii)
      case (StandardType.LongType, vec: BigIntVector, l: Long)      =>
        vec.set(idx, l)
      case (StandardType.FloatType, vec: Float4Vector, f: Float)    =>
        vec.set(idx, f)
      case (StandardType.DoubleType, vec: Float8Vector, d: Double)  =>
        vec.set(idx, d)
      case (other, _, _)                                            =>
        throw EncoderError(s"Unsupported ZIO Schema StandardType $other")
    }

}
