package me.mnedokushev.zio.apache.arrow.core.codec

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.complex.{ ListVector, StructVector }
import org.apache.arrow.vector.complex.impl.UnionListWriter
import org.apache.arrow.vector.types.pojo.{ ArrowType, FieldType }
import org.apache.arrow.vector.{ BigIntVector, BitVector, IntVector, ValueVector, VarCharVector }
import zio.Chunk
import zio.schema.{ Schema, StandardType }

import java.nio.charset.StandardCharsets
import scala.util.control.NonFatal

trait VectorEncoder[-Val, Vector <: ValueVector] extends ArrowEncoder[Val, Vector] {

  override def encode(chunk: Chunk[Val])(implicit alloc: BufferAllocator): Either[Throwable, Vector] =
    try
      Right(encodeUnsafe(chunk))
    catch {
      case NonFatal(ex) => Left(EncoderError("Error encoding vector", Some(ex)))
    }

  protected def encodeUnsafe(chunk: Chunk[Val])(implicit alloc: BufferAllocator): Vector

  protected def init(alloc: BufferAllocator): Vector

}

object VectorEncoder {

  def apply[Val, Vector <: ValueVector](implicit encoder: VectorEncoder[Val, Vector]): VectorEncoder[Val, Vector] =
    encoder

  implicit val booleanEncoder: VectorEncoder[Boolean, BitVector]   =
    scalarEncoder(new BitVector("bitVector", _))(_.allocateNew)(vec => (i, v) => vec.set(i, if (v) 1 else 0))
  implicit val intEncoder: VectorEncoder[Int, IntVector]           =
    scalarEncoder(new IntVector("intVector", _))(_.allocateNew)(_.set)
  implicit val longEncoder: VectorEncoder[Long, BigIntVector]      =
    scalarEncoder(new BigIntVector("longVector", _))(_.allocateNew)(_.set)
  implicit val stringEncoder: VectorEncoder[String, VarCharVector] =
    scalarEncoder(new VarCharVector("stringVector", _))(_.allocateNew)(vec =>
      (i, v) => vec.set(i, v.getBytes(StandardCharsets.UTF_8))
    )

  implicit def listBooleanEncoder[Col[x] <: Iterable[x]]: VectorEncoder[Col[Boolean], ListVector] =
    listEncoder(writer => v => writer.writeBit(if (v) 1 else 0))
  implicit def listIntEncoder[Col[x] <: Iterable[x]]: VectorEncoder[Col[Int], ListVector]         =
    listEncoder(_.writeInt)
  implicit def listLongEncoder[Col[x] <: Iterable[x]]: VectorEncoder[Col[Long], ListVector]       =
    listEncoder(_.writeBigInt)

  def structEncoder[Val](implicit schema: Schema[Val]): VectorEncoder[Val, StructVector] =
    new VectorEncoder[Val, StructVector] {
      override protected def encodeUnsafe(chunk: Chunk[Val])(implicit alloc: BufferAllocator): StructVector = {
        val vec    = init(alloc)
        val len    = chunk.length
        val writer = vec.getWriter
        val it     = chunk.iterator
        var i      = 0

        while (it.hasNext) {
          writer.setPosition(i)

          i += 1
        }

        vec
      }

      override protected def init(alloc: BufferAllocator): StructVector = {
        val vec = StructVector.empty("structVector", alloc)

        schema match {
          case record: Schema.Record[Val] =>
            record.fields.foreach { field =>
              field.schema match {
                case Schema.Primitive(StandardType.IntType, _)    =>
                  vec.addOrGet(
                    field.name,
                    new FieldType(false, new ArrowType.Int(32, true), null),
                    classOf[IntVector]
                  )
                case Schema.Primitive(StandardType.LongType, _)   =>
                  vec.addOrGet(
                    field.name,
                    new FieldType(false, new ArrowType.Int(64, true), null),
                    classOf[BigIntVector]
                  )
                case Schema.Primitive(StandardType.StringType, _) =>
                  vec.addOrGet(
                    field.name,
                    new FieldType(false, ArrowType.Utf8.INSTANCE, null),
                    classOf[VarCharVector]
                  )
                case other                                        =>
                  throw EncoderError(s"Unsupported ZIO Schema type $other")
              }
            }
          case _                          =>
            throw EncoderError(s"Given ZIO schema must be of type Schema.Record[Val]")
        }

        vec
      }

    }

  def listEncoder[Val, Col[x] <: Iterable[x]](
    writeVal: UnionListWriter => Val => Unit
  ): VectorEncoder[Col[Val], ListVector] =
    new VectorEncoder[Col[Val], ListVector] {
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

  def scalarEncoder[Val, Vector <: ValueVector](initVec: BufferAllocator => Vector)(allocNew: Vector => Int => Unit)(
    setVal: Vector => (Int, Val) => Unit
  ): VectorEncoder[Val, Vector] =
    new VectorEncoder[Val, Vector] { self =>
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

}
