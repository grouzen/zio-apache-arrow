package me.mnedokushev.zio.apache.arrow.core.codec

import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.complex.reader.FieldReader
import zio.schema.{ Deriver, DynamicValue, Schema, StandardType }
import zio.{ Chunk, ChunkBuilder }
import scala.annotation.unused
import org.apache.arrow.vector.complex.ListVector
// import org.apache.arrow.vector.complex.ListVector

object ValueVectorDecoderDeriver {

  private def resolveReaderByName(name: Option[String], reader: FieldReader) =
    name.fold[FieldReader](reader.reader())(reader.reader(_))

  private def isNull[V <: ValueVector](@unused reader: FieldReader, vec: V, idx: Int): Boolean =
    vec.isNull(idx)

  def default[V1 <: ValueVector]: Deriver[ValueVectorDecoder[V1, *]] = new Deriver[ValueVectorDecoder[V1, *]] {

    override def deriveRecord[A](
      record: Schema.Record[A],
      fields: => Chunk[Deriver.WrappedF[ValueVectorDecoder[V1, *], _]],
      summoned: => Option[ValueVectorDecoder[V1, A]]
    ): ValueVectorDecoder[V1, A] = new ValueVectorDecoder[V1, A] {

      private val decoders = fields.map(_.unwrap)

      override def decodeUnsafe(vec: V1): Chunk[A] = {
        var idx     = 0
        val len     = vec.getValueCount
        val builder = ChunkBuilder.make[A](len)
        val reader  = vec.getReader

        while (idx < len) {
          reader.setPosition(idx)
          val dynamicValue = ValueDecoder.decodeStruct(record.fields, decoders, reader)

          dynamicValue.toTypedValue(record) match {
            case Right(v)      =>
              builder.addOne(v)
              idx += 1
            case Left(message) =>
              throw DecoderError(message)
          }
        }

        builder.result()
      }

      override def decodeNullableUnsafe(vec: V1): Chunk[Option[A]] = {
        var idx     = 0
        val len     = vec.getValueCount
        val builder = ChunkBuilder.make[Option[A]](len)
        val reader  = vec.getReader

        while (idx < len) {
          if (!isNull(reader, vec, idx)) {
            reader.setPosition(idx)
            val dynamicValue = ValueDecoder.decodeStruct(record.fields, decoders, reader)

            dynamicValue.toTypedValue(record) match {
              case Right(v)      =>
                builder.addOne(Some(v))
                idx += 1
              case Left(message) =>
                throw DecoderError(message)
            }
          } else {
            builder.addOne(None)
          }

          idx += 1
        }

        builder.result()
      }

      def decodeValue(name: Option[String], reader: FieldReader, isNull: Boolean = false): DynamicValue =
        ValueDecoder.decodeStruct(record.fields, decoders, resolveReaderByName(name, reader))

    }

    override def deriveEnum[A](
      `enum`: Schema.Enum[A],
      cases: => Chunk[Deriver.WrappedF[ValueVectorDecoder[V1, *], _]],
      summoned: => Option[ValueVectorDecoder[V1, A]]
    ): ValueVectorDecoder[V1, A] = ???

    override def derivePrimitive[A](
      st: StandardType[A],
      summoned: => Option[ValueVectorDecoder[V1, A]]
    ): ValueVectorDecoder[V1, A] = new ValueVectorDecoder[V1, A] {

      override def decodeUnsafe(vec: V1): Chunk[A] = {
        var idx     = 0
        val len     = vec.getValueCount
        val builder = ChunkBuilder.make[A](len)
        val reader  = vec.getReader

        while (idx < len) {
          reader.setPosition(idx)
          val dynamicValue = ValueDecoder.decodePrimitive(st, reader)

          dynamicValue.toTypedValue(Schema.primitive(st)) match {
            case Right(v)      =>
              builder.addOne(v)
              idx += 1
            case Left(message) =>
              throw DecoderError(message)
          }
        }

        builder.result()
      }

      override def decodeNullableUnsafe(vec: V1): Chunk[Option[A]] = {
        var idx     = 0
        val len     = vec.getValueCount
        val builder = ChunkBuilder.make[Option[A]](len)
        val reader  = vec.getReader

        while (idx < len) {
          if (!isNull(reader, vec, idx)) {
            reader.setPosition(idx)
            val dynamicValue = ValueDecoder.decodePrimitive(st, reader)

            dynamicValue.toTypedValue(Schema.primitive(st)) match {
              case Right(v)      =>
                builder.addOne(Some(v))
              case Left(message) =>
                throw DecoderError(message)
            }
          } else {
            builder.addOne(None)
          }

          idx += 1
        }

        builder.result()
      }

      override def decodeValue(name: Option[String], reader: FieldReader, isNull: Boolean = false): DynamicValue =
        ValueDecoder.decodePrimitive(st, resolveReaderByName(name, reader))

    }

    override def deriveOption[A](
      option: Schema.Optional[A],
      inner: => ValueVectorDecoder[V1, A],
      summoned: => Option[ValueVectorDecoder[V1, Option[A]]]
    ): ValueVectorDecoder[V1, Option[A]] = new ValueVectorDecoder[V1, Option[A]] {

      // TODO: figure out the proper implementation
      override def decodeNullableUnsafe(vec: V1): Chunk[Option[Option[A]]] =
        inner.decodeNullableUnsafe(vec).map(Some(_))

      override def decodeUnsafe(vec: V1): Chunk[Option[A]] =
        inner.decodeNullableUnsafe(vec)

      // TODO: move to ValueDecoder to re-use it in VectorSchemaRootDecoderDeriver (???)
      override def decodeValue(name: Option[String], reader: FieldReader, isNull: Boolean = false): DynamicValue =
        if (isNull)
          DynamicValue.NoneValue
        else
          DynamicValue.SomeValue(inner.decodeValue(name, reader))

    }

    override def deriveSequence[C[_], A](
      sequence: Schema.Sequence[C[A], A, _],
      inner: => ValueVectorDecoder[V1, A],
      summoned: => Option[ValueVectorDecoder[V1, C[A]]]
    ): ValueVectorDecoder[V1, C[A]] = new ValueVectorDecoder[V1, C[A]] {

      override def decodeUnsafe(vec: V1): Chunk[C[A]] = {
        var idx      = 0
        var innerIdx = 0
        val len      = vec.getValueCount
        val builder  = ChunkBuilder.make[C[A]](len)
        val reader   = vec.getReader

        while (idx < len) {
          val innerBuilder = ChunkBuilder.make[A]()

          reader.setPosition(idx)
          while (reader.next()) {
            // NOTE: the hack to check if the element of an inner list is null
            // TODO: figure out a better way to implement this check
            val innerVec     = vec.asInstanceOf[ListVector].getDataVector()
            val dynamicValue = inner.decodeValue(None, reader, isNull(reader, innerVec, innerIdx))

            dynamicValue.toTypedValue(sequence.elementSchema) match {
              case Right(v)      => innerBuilder.addOne(v)
              case Left(message) => throw DecoderError(message)
            }

            innerIdx += 1
          }

          builder.addOne(sequence.fromChunk(innerBuilder.result()))
          idx += 1
        }

        builder.result()
      }

      override def decodeNullableUnsafe(vec: V1): Chunk[Option[C[A]]] = {
        var idx     = 0
        val len     = vec.getValueCount
        val builder = ChunkBuilder.make[Option[C[A]]](len)
        val reader  = vec.getReader

        while (idx < len) {
          if (!isNull(reader, vec, idx)) {
            val innerBuilder = ChunkBuilder.make[A]()

            reader.setPosition(idx)
            while (reader.next()) {
              val dynamicValue = inner.decodeValue(None, reader, isNull(reader, vec, idx))

              dynamicValue.toTypedValue(sequence.elementSchema) match {
                case Right(v)      => innerBuilder.addOne(v)
                case Left(message) => throw DecoderError(message)
              }
            }

            builder.addOne(Some(sequence.fromChunk(innerBuilder.result())))
          } else {
            builder.addOne(None)
          }

          idx += 1
        }

        builder.result()
      }

      override def decodeValue(name: Option[String], reader: FieldReader, isNull: Boolean = false): DynamicValue =
        ValueDecoder.decodeList(inner, resolveReaderByName(name, reader))

    }

    override def deriveMap[K, V](
      map: Schema.Map[K, V],
      key: => ValueVectorDecoder[V1, K],
      value: => ValueVectorDecoder[V1, V],
      summoned: => Option[ValueVectorDecoder[V1, Map[K, V]]]
    ): ValueVectorDecoder[V1, Map[K, V]] = ???

    override def deriveTransformedRecord[A, B](
      record: Schema.Record[A],
      transform: Schema.Transform[A, B, _],
      fields: => Chunk[Deriver.WrappedF[ValueVectorDecoder[V1, *], _]],
      summoned: => Option[ValueVectorDecoder[V1, B]]
    ): ValueVectorDecoder[V1, B] = ???

  }.cached

  def summoned[V1 <: ValueVector]: Deriver[ValueVectorDecoder[V1, *]] =
    default.autoAcceptSummoned

}
