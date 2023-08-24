package me.mnedokushev.zio.apache.arrow.core.codec

import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.reader.FieldReader
import org.apache.arrow.vector.complex.{ ListVector, StructVector }
import zio._
import zio.schema._

import scala.annotation.tailrec
import scala.collection.immutable.ListMap
import scala.util.control.NonFatal

trait ValueVectorDecoder[V <: ValueVector, +A] { self =>

  final def decodeZIO(vec: V): Task[Chunk[A]] =
    ZIO.fromEither(decode(vec))

  final def decode(vec: V): Either[Throwable, Chunk[A]] =
    try
      Right(decodeUnsafe(vec))
    catch {
      case decoderError: DecoderError => Left(decoderError)
      case NonFatal(ex)               => Left(DecoderError("Error decoding vector", Some(ex)))
    }

  protected def decodeUnsafe(vec: V): Chunk[A]

  final def map[B](f: A => B): ValueVectorDecoder[V, B] =
    new ValueVectorDecoder[V, B] {
      override protected def decodeUnsafe(vec: V): Chunk[B] =
        self.decodeUnsafe(vec).map(f)
    }

}

object ValueVectorDecoder {

  def apply[V <: ValueVector, A](implicit decoder: ValueVectorDecoder[V, A]): ValueVectorDecoder[V, A] =
    decoder

  implicit def primitive[V <: ValueVector, A](implicit schema: Schema[A]): ValueVectorDecoder[V, A] =
    new ValueVectorDecoder[V, A] {
      override protected def decodeUnsafe(vec: V): Chunk[A] =
        schema match {
          case Schema.Primitive(standardType, _) =>
            var idx     = 0
            val len     = vec.getValueCount
            val builder = ChunkBuilder.make[A](len)
            val reader  = vec.getReader

            while (idx < len) {
              reader.setPosition(idx)
              val dynamicValue = decodePrimitive(standardType, reader)

              dynamicValue.toTypedValue match {
                case Right(v)      =>
                  builder.addOne(v)
                  idx += 1
                case Left(message) =>
                  throw DecoderError(message)
              }
            }

            builder.result()
          case _                                 =>
            throw DecoderError(s"Given ZIO schema must be of type Schema.Primitive[Val]")
        }
    }

  implicit def list[A](implicit schema: Schema[A]): ValueVectorDecoder[ListVector, Chunk[A]] =
    new ValueVectorDecoder[ListVector, Chunk[A]] {
      override protected def decodeUnsafe(vec: ListVector): Chunk[Chunk[A]] = {
        var idx     = 0
        val len     = vec.getValueCount
        val builder = ChunkBuilder.make[Chunk[A]](len)
        val reader  = vec.getReader

        while (idx < len) {
          val innerBuilder = ChunkBuilder.make[A]()

          reader.setPosition(idx)
          while (reader.next())
            if (reader.isSet) {
              val dynamicValue = decodeSchema(None, schema, reader)

              dynamicValue.toTypedValue match {
                case Right(v)      => innerBuilder.addOne(v)
                case Left(message) => throw DecoderError(message)
              }
            }

          builder.addOne(innerBuilder.result())
          idx += 1
        }

        builder.result()
      }
    }

  implicit def struct[A](implicit schema: Schema[A]): ValueVectorDecoder[StructVector, A] =
    new ValueVectorDecoder[StructVector, A] {
      override protected def decodeUnsafe(vec: StructVector): Chunk[A] =
        schema match {
          case record: Schema.Record[A] =>
            var idx     = 0
            val len     = vec.getValueCount
            val builder = ChunkBuilder.make[A](len)
            val reader  = vec.getReader

            while (idx < len) {
              reader.setPosition(idx)
              val dynamicValue = decodeCaseClass(record.fields, reader)

              dynamicValue.toTypedValue match {
                case Right(v)      =>
                  builder.addOne(v)
                  idx += 1
                case Left(message) =>
                  throw DecoderError(message)
              }
            }

            builder.result()
          case _                        =>
            throw DecoderError(s"Given ZIO schema must be of type Schema.Record[Val]")
        }
    }

  @tailrec
  private[codec] def decodeSchema[A](name: Option[String], schema: Schema[A], reader: FieldReader): DynamicValue = {
    val reader0 = name.fold[FieldReader](reader.reader())(reader.reader(_))

    schema match {
      case Schema.Primitive(standardType, _)       =>
        decodePrimitive(standardType, reader0)
      case record: Schema.Record[A]                =>
        decodeCaseClass(record.fields, reader0)
      case Schema.Sequence(elemSchema, _, _, _, _) =>
        decodeSequence(elemSchema, reader0)
      case lzy: Schema.Lazy[_]                     =>
        decodeSchema(name, lzy.schema, reader)
      case other                                   =>
        throw DecoderError(s"Unsupported ZIO Schema type $other")
    }
  }

  private[codec] def decodeCaseClass[A](fields: Chunk[Schema.Field[A, _]], reader: FieldReader): DynamicValue = {
    val values = ListMap(fields.map { case Schema.Field(name, schema0, _, _, _, _) =>
      val value: DynamicValue = decodeSchema(Some(name), schema0, reader)

      name -> value
    }: _*)

    DynamicValue.Record(TypeId.Structural, values)
  }

  private[codec] def decodeSequence[A](schema: Schema[A], reader: FieldReader): DynamicValue = {
    val builder = ChunkBuilder.make[DynamicValue]()

    while (reader.next())
      if (reader.isSet)
        builder.addOne(decodeSchema(None, schema, reader))

    DynamicValue.Sequence(builder.result())
  }

  private[codec] def decodePrimitive[A](standardType: StandardType[A], reader: FieldReader): DynamicValue =
    standardType match {
      case t: StandardType.BoolType.type   =>
        DynamicValue.Primitive[Boolean](reader.readBoolean(), t)
      case t: StandardType.IntType.type    =>
        DynamicValue.Primitive[Int](reader.readInteger(), t)
      case t: StandardType.LongType.type   =>
        DynamicValue.Primitive[Long](reader.readLong(), t)
      case t: StandardType.FloatType.type  =>
        DynamicValue.Primitive[Float](reader.readFloat(), t)
      case t: StandardType.DoubleType.type =>
        DynamicValue.Primitive[Double](reader.readDouble(), t)
      case t: StandardType.StringType.type =>
        DynamicValue.Primitive[String](reader.readText().toString, t)
      case other                           =>
        throw DecoderError(s"Unsupported ZIO Schema type $other")
    }

}
