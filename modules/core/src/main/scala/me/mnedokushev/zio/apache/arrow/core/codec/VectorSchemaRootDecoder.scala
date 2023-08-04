package me.mnedokushev.zio.apache.arrow.core.codec

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.complex.reader.FieldReader
import zio._
import zio.schema.{ DynamicValue, Schema, TypeId }

import scala.annotation.tailrec
import scala.collection.immutable.ListMap
import scala.util.control.NonFatal

trait VectorSchemaRootDecoder[+Val] { self =>

  final def decodeZIO(rootVec: VectorSchemaRoot): Task[Chunk[Val]] =
    ZIO.fromEither(decode(rootVec))

  final def decode(rootVec: VectorSchemaRoot): Either[Throwable, Chunk[Val]] =
    try
      Right(decodeUnsafe(rootVec))
    catch {
      case decoderError: DecoderError => Left(decoderError)
      case NonFatal(ex)               => Left(DecoderError("Error decoding vector schema root", Some(ex)))
    }

  protected def decodeUnsafe(rootVec: VectorSchemaRoot): Chunk[Val]

//
//  final def flatMap[B](f: Val => VectorSchemaRootDecoder[B]): VectorSchemaRootDecoder[B] =
//    new VectorSchemaRootDecoder[B] {
//      override def decodeUnsafe(from: VectorSchemaRoot, idx: Int): B =
//        f(self.decodeUnsafe(from, idx)).decodeUnsafe(from, idx)
//    }
//
//  final def map[B](f: Val => B): VectorSchemaRootDecoder[B] =
//    new VectorSchemaRootDecoder[B] {
//      override def decodeUnsafe(from: VectorSchemaRoot, idx: Int): B =
//        f(self.decodeUnsafe(from, idx))
//    }

}

object VectorSchemaRootDecoder {

  implicit def schema[Val](implicit schema: Schema[Val]): VectorSchemaRootDecoder[Val] =
    new VectorSchemaRootDecoder[Val] {
      override protected def decodeUnsafe(rootVec: VectorSchemaRoot): Chunk[Val] = {
        @tailrec
        def decodeField[A](fieldSchema: Schema[A], reader: FieldReader): DynamicValue =
          fieldSchema match {
            case Schema.Primitive(standardType, _)       =>
              ValueVectorDecoder.decodePrimitive(standardType, reader)
            case record: Schema.Record[A]                =>
              ValueVectorDecoder.decodeCaseClass(record.fields, reader)
            case Schema.Sequence(elemSchema, _, _, _, _) =>
              ValueVectorDecoder.decodeSequence(elemSchema, reader)
            case lzy: Schema.Lazy[_]                     =>
              decodeField(lzy.schema, reader)
            case other                                   =>
              throw DecoderError(s"Unsupported ZIO Schema type $other")
          }

        schema match {
          case record: Schema.Record[Val] =>
            SchemaEncoder.schemaRoot[Val] match {
              case Right(s) if s == rootVec.getSchema =>
                val fields = record.fields.map { case Schema.Field(name, fieldSchema, _, _, _, _) =>
                  val vec    = Option(rootVec.getVector(name))
                    .getOrElse(throw DecoderError(s"Couldn't get vector by name $name"))
                  val reader = vec.getReader

                  (fieldSchema, name, reader)
                }

                var idx     = 0
                val len     = rootVec.getRowCount
                val builder = ChunkBuilder.make[Val]()

                while (idx < len) {
                  val values = fields.map { case (fieldSchema, name, reader) =>
                    reader.setPosition(idx)
                    val value = decodeField(fieldSchema, reader)

                    name -> value
                  }.to(ListMap)

                  DynamicValue.Record(TypeId.Structural, values).toTypedValue match {
                    case Right(v)      =>
                      builder.addOne(v)
                      idx += 1
                    case Left(message) =>
                      throw DecoderError(message)
                  }
                }

                builder.result()
              case Right(s)                           =>
                throw DecoderError(s"Schemas are not equal $s != ${rootVec.getSchema}")
              case Left(error)                        =>
                throw error
            }
          case _                          =>
            throw DecoderError(s"Given ZIO schema must be of type Schema.Record[Val]")
        }
      }
    }

}
