package me.mnedokushev.zio.apache.arrow.core.vector

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.types.pojo.{ ArrowType, FieldType }
import zio.schema._
import zio._

abstract class ZVectorStruct[Val](implicit schema: Schema[Val]) extends ZVector[Val, StructVector] {

  def empty: RIO[Scope with BufferAllocator, StructVector] =
    fromUnsafe(Unsafe.empty(_))

  object Unsafe {

    def empty(implicit alloc: BufferAllocator): StructVector = {
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
                throw VectorError(s"Unsupported ZIO Schema type $other")
            }
          }
        case _                          =>
          throw VectorError(s"Given ZIO schema must be of type Schema.Record[Val]")
      }

      vec
    }

    def fromIterable(it: Iterable[Val])(implicit alloc: BufferAllocator): StructVector = {
      val vec    = this.empty
      val len    = it.size
      val writer = vec.getWriter

      it.zipWithIndex.foreach { case (v, i) =>
        writer.setPosition(i)

      }

      ???
    }

  }

}
