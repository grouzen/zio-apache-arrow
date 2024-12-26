package me.mnedokushev.zio.apache.arrow.core.codec

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.complex.impl.{ PromotableWriter, UnionListWriter }
import org.apache.arrow.vector.complex.writer.FieldWriter
import org.apache.arrow.vector.complex.{ ListVector, StructVector }
import org.apache.arrow.vector.{ FieldVector, VectorSchemaRoot }
import zio.Chunk
import zio.schema.{ Deriver, Schema, StandardType }

object VectorSchemaRootEncoderDeriver {

  val default: Deriver[VectorSchemaRootEncoder] = new Deriver[VectorSchemaRootEncoder] {

    override def deriveRecord[A](
      record: Schema.Record[A],
      fields: => Chunk[Deriver.WrappedF[VectorSchemaRootEncoder, ?]],
      summoned: => Option[VectorSchemaRootEncoder[A]]
    ): VectorSchemaRootEncoder[A] = new VectorSchemaRootEncoder[A] {

      private val encoders = fields.map(_.unwrap)

      private def encodeField0[A1](
        encoder: VectorSchemaRootEncoder[?],
        value: A1,
        writer: FieldWriter
      )(implicit
        alloc: BufferAllocator
      ) =
        encoder.asInstanceOf[VectorSchemaRootEncoder[A1]].encodeField(value, writer)

      override protected def encodeUnsafe(
        chunk: Chunk[A],
        root: VectorSchemaRoot
      )(implicit alloc: BufferAllocator): VectorSchemaRoot = {
        val fields0 =
          record.fields.zip(encoders).map { case (Schema.Field(name, _, _, _, g, _), encoder) =>
            val vec = Option(root.getVector(name))
              .getOrElse(throw EncoderError(s"Couldn't find vector by name $name"))

            vec.reset()

            // val writer = resolveWriter(fieldSchema, vec)
            val writer = encoder.getWriter(vec)

            (encoder, vec, writer, g)
          }

        val len = chunk.length
        val it  = chunk.iterator.zipWithIndex

        it.foreach { case (v, i) =>
          fields0.foreach { case (encoder, _, writer, get) =>
            writer.setPosition(i)
            encodeField0(encoder, get(v), writer)
          }
        }

        fields0.foreach { case (_, vec, _, _) =>
          vec.setValueCount(len)
        }

        root.setRowCount(len)
        root
      }

      def encodeValue(
        value: A,
        name: Option[String],
        writer: FieldWriter
      )(implicit alloc: BufferAllocator): Unit = {
        val writer0 = name.fold[FieldWriter](writer.struct().asInstanceOf[UnionListWriter])(
          writer.struct(_).asInstanceOf[PromotableWriter]
        )

        ValueEncoder.encodeStruct(value, record.fields, encoders, writer0)
      }

      override def encodeField(value: A, writer: FieldWriter)(implicit alloc: BufferAllocator): Unit =
        ValueEncoder.encodeStruct(value, record.fields, encoders, writer)

      override def getWriter(vec: FieldVector): FieldWriter =
        vec.asInstanceOf[StructVector].getWriter

    }

    override def deriveEnum[A](
      `enum`: Schema.Enum[A],
      cases: => Chunk[Deriver.WrappedF[VectorSchemaRootEncoder, ?]],
      summoned: => Option[VectorSchemaRootEncoder[A]]
    ): VectorSchemaRootEncoder[A] = ???

    override def derivePrimitive[A](
      st: StandardType[A],
      summoned: => Option[VectorSchemaRootEncoder[A]]
    ): VectorSchemaRootEncoder[A] =
      VectorSchemaRootEncoder.primitive[A](
        encodeValue0 = (v, name, writer, alloc) => ValueEncoder.encodePrimitive(st, v, name, writer)(alloc),
        encodeField0 = (v, writer, alloc) => ValueEncoder.encodePrimitive(st, v, writer)(alloc),
        getWriter0 = vec => primitiveWriter(st, vec)
      )(st)

    override def deriveOption[A](
      option: Schema.Optional[A],
      inner: => VectorSchemaRootEncoder[A],
      summoned: => Option[VectorSchemaRootEncoder[Option[A]]]
    ): VectorSchemaRootEncoder[Option[A]] = new VectorSchemaRootEncoder[Option[A]] {

      override def encodeValue(value: Option[A], name: Option[String], writer: FieldWriter)(implicit
        alloc: BufferAllocator
      ): Unit =
        value match {
          case Some(value0) =>
            inner.encodeValue(value0, name, writer)
          case None         =>
            writer.writeNull()
        }

      override def encodeField(value: Option[A], writer: FieldWriter)(implicit alloc: BufferAllocator): Unit =
        value match {
          case Some(value0) =>
            inner.encodeField(value0, writer)
          case None         =>
            writer.writeNull()
        }

      override def getWriter(vec: FieldVector): FieldWriter =
        inner.getWriter(vec)

    }

    override def deriveSequence[C[_], A](
      sequence: Schema.Sequence[C[A], A, ?],
      inner: => VectorSchemaRootEncoder[A],
      summoned: => Option[VectorSchemaRootEncoder[C[A]]]
    ): VectorSchemaRootEncoder[C[A]] = new VectorSchemaRootEncoder[C[A]] {

      override def encodeValue(value: C[A], name: Option[String], writer: FieldWriter)(implicit
        alloc: BufferAllocator
      ): Unit = {
        val writer0 = name.fold(writer.list)(writer.list).asInstanceOf[PromotableWriter]

        ValueEncoder.encodeList(sequence.toChunk(value), inner, writer0)
      }

      override def encodeField(value: C[A], writer: FieldWriter)(implicit alloc: BufferAllocator): Unit =
        ValueEncoder.encodeList(sequence.toChunk(value), inner, writer)

      override def getWriter(vec: FieldVector): FieldWriter =
        vec.asInstanceOf[ListVector].getWriter

    }

    override def deriveMap[K, V](
      map: Schema.Map[K, V],
      key: => VectorSchemaRootEncoder[K],
      value: => VectorSchemaRootEncoder[V],
      summoned: => Option[VectorSchemaRootEncoder[Map[K, V]]]
    ): VectorSchemaRootEncoder[Map[K, V]] = ???

    override def deriveTransformedRecord[A, B](
      record: Schema.Record[A],
      transform: Schema.Transform[A, B, ?],
      fields: => Chunk[Deriver.WrappedF[VectorSchemaRootEncoder, ?]],
      summoned: => Option[VectorSchemaRootEncoder[B]]
    ): VectorSchemaRootEncoder[B] = ???

  }.cached

  def summoned: Deriver[VectorSchemaRootEncoder] = default.autoAcceptSummoned

}
