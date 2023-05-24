package me.mnedokushev.zio.apache.arrow.core.codec

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.complex.{ ListVector, StructVector }
import org.apache.arrow.vector.complex.impl.UnionListWriter
import org.apache.arrow.vector.types.pojo.{ ArrowType, FieldType }
import org.apache.arrow.vector._
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

  implicit def listBooleanEncoder[Col[x] <: Iterable[x]]: ArrowVectorEncoder[Col[Boolean], ListVector] =
    list(writer => v => writer.writeBit(if (v) 1 else 0))
  implicit def listIntEncoder[Col[x] <: Iterable[x]]: ArrowVectorEncoder[Col[Int], ListVector]         =
    list(_.writeInt)
  implicit def listLongEncoder[Col[x] <: Iterable[x]]: ArrowVectorEncoder[Col[Long], ListVector]       =
    list(_.writeBigInt)

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
        val vec    = init(alloc)
        val len    = chunk.length
        val writer = vec.getWriter
        val it     = chunk.iterator
        var i      = 0

        while (it.hasNext) {
          writer.setPosition(i)
          // TODO: implement
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
                  throw ArrowEncoderError(s"Unsupported ZIO Schema type $other")
              }
            }
          case _                          =>
            throw ArrowEncoderError(s"Given ZIO schema must be of type Schema.Record[Val]")
        }

        vec
      }

    }

}
