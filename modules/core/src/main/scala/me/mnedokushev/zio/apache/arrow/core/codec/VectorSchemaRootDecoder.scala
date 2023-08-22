package me.mnedokushev.zio.apache.arrow.core.codec

import me.mnedokushev.zio.apache.arrow.core._
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.complex.reader.FieldReader
import zio._
import zio.schema.{ DynamicValue, Schema, TypeId }

import scala.annotation.tailrec
import scala.collection.immutable.ListMap
import scala.util.control.NonFatal

trait VectorSchemaRootDecoder[A] { self =>

  final def decodeZIO(root: VectorSchemaRoot): Task[Chunk[A]] =
    ZIO.fromEither(decode(root))

  final def decode(root: VectorSchemaRoot): Either[Throwable, Chunk[A]] =
    try
      Right(decodeUnsafe(root))
    catch {
      case decoderError: DecoderError => Left(decoderError)
      case NonFatal(ex)               => Left(DecoderError("Error decoding vector schema root", Some(ex)))
    }

  protected def decodeUnsafe(root: VectorSchemaRoot): Chunk[A]

  final def map[B](f: A => B): VectorSchemaRootDecoder[B] =
    new VectorSchemaRootDecoder[B] {
      override def decodeUnsafe(root: VectorSchemaRoot): Chunk[B] =
        self.decodeUnsafe(root).map(f)
    }

}

object VectorSchemaRootDecoder {

  def apply[A](implicit decoder: VectorSchemaRootDecoder[A]): VectorSchemaRootDecoder[A] =
    decoder

  implicit def schema[A](implicit schema: Schema[A]): VectorSchemaRootDecoder[A] =
    new VectorSchemaRootDecoder[A] {
      override protected def decodeUnsafe(root: VectorSchemaRoot): Chunk[A] = {
        @tailrec
        def decodeField[A1](fieldSchema: Schema[A1], reader: FieldReader): DynamicValue =
          fieldSchema match {
            case Schema.Primitive(standardType, _)       =>
              ValueVectorDecoder.decodePrimitive(standardType, reader)
            case record: Schema.Record[A1]                =>
              ValueVectorDecoder.decodeCaseClass(record.fields, reader)
            case Schema.Sequence(elemSchema, _, _, _, _) =>
              ValueVectorDecoder.decodeSequence(elemSchema, reader)
            case lzy: Schema.Lazy[_]                     =>
              decodeField(lzy.schema, reader)
            case other                                   =>
              throw DecoderError(s"Unsupported ZIO Schema type $other")
          }

        schema match {
          case record: Schema.Record[A] =>
            validateSchema(root.getSchema) {
              val fields = record.fields.map { case Schema.Field(name, fieldSchema, _, _, _, _) =>
                val vec    = Option(root.getVector(name))
                  .getOrElse(throw DecoderError(s"Couldn't get vector by name $name"))
                val reader = vec.getReader

                (fieldSchema, name, reader)
              }

              var idx     = 0
              val len     = root.getRowCount
              val builder = ChunkBuilder.make[A]()

              while (idx < len) {
                val values = ListMap(fields.map { case (fieldSchema, name, reader) =>
                  reader.setPosition(idx)
                  val value = decodeField(fieldSchema, reader)

                  name -> value
                }: _*)

                DynamicValue.Record(TypeId.Structural, values).toTypedValue match {
                  case Right(v)      =>
                    builder.addOne(v)
                    idx += 1
                  case Left(message) =>
                    throw DecoderError(message)
                }
              }

              builder.result()
            }
          case _                          =>
            throw DecoderError(s"Given ZIO schema must be of type Schema.Record[Val]")
        }
      }
    }

}
