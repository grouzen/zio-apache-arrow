package me.mnedokushev.zio.apache.arrow.core.codec

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.impl.{ PromotableWriter, UnionListWriter }
import org.apache.arrow.vector.complex.writer.FieldWriter
import org.apache.arrow.vector.complex.{ ListVector, StructVector }
import zio._
import zio.schema._

import java.nio.charset.StandardCharsets
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
        val buffer = alloc.buffer(s.length.toLong)
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
