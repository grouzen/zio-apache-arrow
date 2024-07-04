package me.mnedokushev.zio.apache.arrow.core.codec

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.complex.reader.FieldReader
import zio.schema.{ Deriver, DynamicValue, Schema, StandardType, TypeId }
import zio.{ Chunk, ChunkBuilder }

import scala.collection.immutable.ListMap

object VectorSchemaRootDecoderDeriver {

  val default: Deriver[VectorSchemaRootDecoder] = new Deriver[VectorSchemaRootDecoder] {

    override def deriveRecord[A](
      record: Schema.Record[A],
      fields: => Chunk[Deriver.WrappedF[VectorSchemaRootDecoder, _]],
      summoned: => Option[VectorSchemaRootDecoder[A]]
    ): VectorSchemaRootDecoder[A] = new VectorSchemaRootDecoder[A] {

      private val decoders = fields.map(_.unwrap)

      override protected def decodeUnsafe(root: VectorSchemaRoot): Chunk[A] = {
        val fields0 = record.fields.zip(decoders).map { case (field, decoder) =>
          val vec    =
            Option(root.getVector(field.name))
              .getOrElse(throw DecoderError(s"Couldn't get vector by name ${field.name}"))
          val reader = vec.getReader

          (decoder, field.name.toString, reader)
        }

        var idx     = 0
        val len     = root.getRowCount
        val builder = ChunkBuilder.make[A]()

        while (idx < len) {
          val values = ListMap(fields0.map { case (decoder, name, reader) =>
            reader.setPosition(idx)
            val value = decoder.decodeField(reader)

            name.toString -> value
          }: _*)

          DynamicValue.Record(TypeId.Structural, values).toTypedValue(record) match {
            case Right(v)      =>
              builder.addOne(v)
              idx += 1
            case Left(message) =>
              throw DecoderError(message)
          }
        }

        builder.result()
      }

      override def decodeValue(name: Option[String], reader: FieldReader, isNull: Boolean = false): DynamicValue =
        ValueDecoder.decodeStruct(record.fields, decoders, reader)

    }

    override def deriveEnum[A](
      `enum`: Schema.Enum[A],
      cases: => Chunk[Deriver.WrappedF[VectorSchemaRootDecoder, _]],
      summoned: => Option[VectorSchemaRootDecoder[A]]
    ): VectorSchemaRootDecoder[A] = ???

    override def derivePrimitive[A](
      st: StandardType[A],
      summoned: => Option[VectorSchemaRootDecoder[A]]
    ): VectorSchemaRootDecoder[A] = new VectorSchemaRootDecoder[A] {

      override def decodeValue(name: Option[String], reader: FieldReader, isNull: Boolean = false): DynamicValue =
        ValueDecoder.decodePrimitive(st, reader)

    }

    override def deriveOption[A](
      option: Schema.Optional[A],
      inner: => VectorSchemaRootDecoder[A],
      summoned: => Option[VectorSchemaRootDecoder[Option[A]]]
    ): VectorSchemaRootDecoder[Option[A]] = ???

    override def deriveSequence[C[_], A](
      sequence: Schema.Sequence[C[A], A, _],
      inner: => VectorSchemaRootDecoder[A],
      summoned: => Option[VectorSchemaRootDecoder[C[A]]]
    ): VectorSchemaRootDecoder[C[A]] = new VectorSchemaRootDecoder[C[A]] {

      override def decodeValue(name: Option[String], reader: FieldReader, isNull: Boolean = false): DynamicValue =
        ValueDecoder.decodeList(inner, reader)

    }

    override def deriveMap[K, V](
      map: Schema.Map[K, V],
      key: => VectorSchemaRootDecoder[K],
      value: => VectorSchemaRootDecoder[V],
      summoned: => Option[VectorSchemaRootDecoder[Map[K, V]]]
    ): VectorSchemaRootDecoder[Map[K, V]] = ???

    override def deriveTransformedRecord[A, B](
      record: Schema.Record[A],
      transform: Schema.Transform[A, B, _],
      fields: => Chunk[Deriver.WrappedF[VectorSchemaRootDecoder, _]],
      summoned: => Option[VectorSchemaRootDecoder[B]]
    ): VectorSchemaRootDecoder[B] = ???

  }.cached

  def summoned: Deriver[VectorSchemaRootDecoder] = default.autoAcceptSummoned

}
