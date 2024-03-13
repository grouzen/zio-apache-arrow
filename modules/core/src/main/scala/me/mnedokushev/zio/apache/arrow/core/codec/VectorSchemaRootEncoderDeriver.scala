package me.mnedokushev.zio.apache.arrow.core.codec

import zio.schema.Deriver
import zio.schema.Schema
import zio.Chunk
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.complex.writer.FieldWriter
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.FieldVector
import zio.schema.StandardType

object VectorSchemaRootEncoderDeriver {

  val default: Deriver[VectorSchemaRootEncoder] = new Deriver[VectorSchemaRootEncoder] {

    override def deriveRecord[A](
      record: Schema.Record[A],
      fields: => Chunk[Deriver.WrappedF[VectorSchemaRootEncoder, _]],
      summoned: => Option[VectorSchemaRootEncoder[A]]
    ): VectorSchemaRootEncoder[A] = new VectorSchemaRootEncoder[A] {

      override def encodeField(name: String, vec: FieldVector, writer: FieldWriter, value: A, idx: Int)(implicit
        alloc: BufferAllocator
      ): Unit =
        ValueEncoder.encodeStruct(value, record.fields, fields.map(_.unwrap), writer)

      private def encodeField0[A1](
        encoder: VectorSchemaRootEncoder[_],
        name: String,
        vec: FieldVector,
        writer: FieldWriter,
        value: A1,
        idx: Int
      )(implicit
        alloc: BufferAllocator
      ) =
        encoder.asInstanceOf[VectorSchemaRootEncoder[A1]].encodeField(name, vec, writer, value, idx)

      override protected def encodeUnsafe(
        chunk: Chunk[A],
        root: VectorSchemaRoot
      )(implicit alloc: BufferAllocator): VectorSchemaRoot = {
        val fields0 =
          record.fields.zip(fields.map(_.unwrap)).map { case (Schema.Field(name, fieldSchema, _, _, g, _), encoder) =>
            val vec = Option(root.getVector(name))
              .getOrElse(throw EncoderError(s"Couldn't find vector by name $name"))

            vec.reset()

            val writer: FieldWriter = (fieldSchema, vec) match {
              case (_: Schema.Record[_], vec0: StructVector)       => vec0.getWriter
              case (_: Schema.Sequence[_, _, _], vec0: ListVector) => vec0.getWriter
              case _                                               => null
            }

            (encoder, name, vec, writer, g)
          }

        val len = chunk.length
        val it  = chunk.iterator.zipWithIndex

        it.foreach { case (v, i) =>
          fields0.foreach { case (encoder, name, vec, writer, get) =>
            encodeField0(encoder, name, vec, writer, get(v), i)
          }
        }

        fields0.foreach { case (_, _, vec, _, _) =>
          vec.setValueCount(len)
        }

        root.setRowCount(len)
        root
      }

    }

    override def deriveEnum[A](
      enum: Schema.Enum[A],
      cases: => Chunk[Deriver.WrappedF[VectorSchemaRootEncoder, _]],
      summoned: => Option[VectorSchemaRootEncoder[A]]
    ): VectorSchemaRootEncoder[A] = ???

    override def derivePrimitive[A](
      st: StandardType[A],
      summoned: => Option[VectorSchemaRootEncoder[A]]
    ): VectorSchemaRootEncoder[A] = new VectorSchemaRootEncoder[A] {

      override def encodeField(name: String, vec: FieldVector, writer: FieldWriter, value: A, idx: Int)(implicit
        alloc: BufferAllocator
      ): Unit =
        ValueEncoder.encodePrimitive(st, value, vec, idx)

    }

    override def deriveOption[A](
      option: Schema.Optional[A],
      inner: => VectorSchemaRootEncoder[A],
      summoned: => Option[VectorSchemaRootEncoder[Option[A]]]
    ): VectorSchemaRootEncoder[Option[A]] = ???

    override def deriveSequence[C[_], A](
      sequence: Schema.Sequence[C[A], A, _],
      inner: => VectorSchemaRootEncoder[A],
      summoned: => Option[VectorSchemaRootEncoder[C[A]]]
    ): VectorSchemaRootEncoder[C[A]] = new VectorSchemaRootEncoder[C[A]] {

      override def encodeField(name: String, vec: FieldVector, writer: FieldWriter, value: C[A], idx: Int)(implicit
        alloc: BufferAllocator
      ): Unit =
        ValueEncoder.encodeList(sequence.toChunk(value), inner, writer)

    }

    override def deriveMap[K, V](
      map: Schema.Map[K, V],
      key: => VectorSchemaRootEncoder[K],
      value: => VectorSchemaRootEncoder[V],
      summoned: => Option[VectorSchemaRootEncoder[Map[K, V]]]
    ): VectorSchemaRootEncoder[Map[K, V]] = ???

    override def deriveTransformedRecord[A, B](
      record: Schema.Record[A],
      transform: Schema.Transform[A, B, _],
      fields: => Chunk[Deriver.WrappedF[VectorSchemaRootEncoder, _]],
      summoned: => Option[VectorSchemaRootEncoder[B]]
    ): VectorSchemaRootEncoder[B] = ???

  }

}
