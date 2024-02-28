package me.mnedokushev.zio.apache.arrow.core.codec

import me.mnedokushev.zio.apache.arrow.core._
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.complex.writer.FieldWriter
import org.apache.arrow.vector.complex.{ ListVector, StructVector }
import org.apache.arrow.vector.{ FieldVector, VectorSchemaRoot }
import zio._
import zio.schema.Schema

import scala.annotation.tailrec
import scala.util.control.NonFatal

trait VectorSchemaRootEncoder[-A] { self =>

  final def encodeZIO(chunk: Chunk[A], root: VectorSchemaRoot): RIO[Scope with BufferAllocator, VectorSchemaRoot] =
    ZIO.fromAutoCloseable(
      ZIO.serviceWithZIO[BufferAllocator] { implicit alloc =>
        ZIO.fromEither(encode(chunk, root))
      }
    )

  final def encode(
    chunk: Chunk[A],
    root: VectorSchemaRoot
  )(implicit alloc: BufferAllocator): Either[Throwable, VectorSchemaRoot] =
    try
      Right(encodeUnsafe(chunk, root))
    catch {
      case encoderError: EncoderError => Left(encoderError)
      case NonFatal(ex)               => Left(EncoderError("Error encoding vector schema root", Some(ex)))
    }

  protected def encodeUnsafe(
    chunk: Chunk[A],
    root: VectorSchemaRoot
  )(implicit alloc: BufferAllocator): VectorSchemaRoot

  final def contramap[B](f: B => A): VectorSchemaRootEncoder[B] =
    new VectorSchemaRootEncoder[B] {
      override protected def encodeUnsafe(chunk: Chunk[B], root: VectorSchemaRoot)(implicit
        alloc: BufferAllocator
      ): VectorSchemaRoot =
        self.encodeUnsafe(chunk.map(f), root)
    }

}

object VectorSchemaRootEncoder {

  def apply[A](implicit encoder: VectorSchemaRootEncoder[A]): VectorSchemaRootEncoder[A] =
    encoder

  implicit def schema[A: SchemaEncoder](implicit schema: Schema[A]): VectorSchemaRootEncoder[A] =
    new VectorSchemaRootEncoder[A] {
      override protected def encodeUnsafe(
        chunk: Chunk[A],
        root: VectorSchemaRoot
      )(implicit alloc: BufferAllocator): VectorSchemaRoot = {
        @tailrec
        def encodeField[A1](
          fieldSchema: Schema[A1],
          name: String,
          vec: FieldVector,
          writer: FieldWriter,
          value: A1,
          idx: Int
        ): Unit =
          fieldSchema match {
            case Schema.Primitive(standardType, _)          =>
              ValueVectorEncoder.encodePrimitive(value, standardType, vec, idx)
            case record: Schema.Record[_]                   =>
              ValueVectorEncoder.encodeCaseClass(value, record.fields, writer)
            case Schema.Sequence(elementSchema, _, g, _, _) =>
              ValueVectorEncoder.encodeSequence(g(value), elementSchema, writer)
            case lzy: Schema.Lazy[_]                        =>
              encodeField(lzy.schema, name, vec, writer, value, idx)
            case other                                      =>
              throw EncoderError(s"Unsupported ZIO Schema type $other")
          }

        schema match {
          case record: Schema.Record[A] =>
            whenSchemaValid(root.getSchema) {
              val fields = record.fields.map { case Schema.Field(name, fieldSchema, _, _, g, _) =>
                val vec = Option(root.getVector(name))
                  .getOrElse(throw EncoderError(s"Couldn't find vector by name $name"))

                vec.reset()

                val writer: FieldWriter = (fieldSchema, vec) match {
                  case (_: Schema.Record[_], vec0: StructVector)       => vec0.getWriter
                  case (_: Schema.Sequence[_, _, _], vec0: ListVector) => vec0.getWriter
                  case _                                               => null
                }

                (fieldSchema, name, vec, writer, g)
              }

              val len = chunk.length
              val it  = chunk.iterator.zipWithIndex

              it.foreach { case (v, i) =>
                fields.foreach { case (fieldSchema, name, vec, writer, get) =>
                  encodeField(fieldSchema.asInstanceOf[Schema[Any]], name, vec, writer, get(v), i)
                }
              }

              fields.foreach { case (_, _, vec, _, _) =>
                vec.setValueCount(len)
              }

              root.setRowCount(len)
              root
            }
          case _                        =>
            throw EncoderError(s"Given ZIO schema must be of type Schema.Record[Val]")
        }
      }
    }

}
