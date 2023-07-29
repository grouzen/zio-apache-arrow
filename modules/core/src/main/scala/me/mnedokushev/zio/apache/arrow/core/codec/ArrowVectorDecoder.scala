package me.mnedokushev.zio.apache.arrow.core.codec

import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.{ ListVector, StructVector }
import org.apache.arrow.vector.complex.impl._
import org.apache.arrow.vector.complex.reader.FieldReader
import zio._
import zio.schema._

import java.nio.charset.StandardCharsets
import scala.annotation.tailrec
import scala.collection.immutable.ListMap
import scala.collection.mutable.ListBuffer
import scala.util.control.NonFatal

trait ArrowVectorDecoder[Vector <: ValueVector, +Val] extends ArrowDecoder[Vector, Val] { self =>

  override final def decode(from: Vector): Either[Throwable, Chunk[Val]] =
    try {
      var idx        = 0
      val valueCount = from.getValueCount
      val builder    = ChunkBuilder.make[Val](valueCount)

      while (idx < valueCount) {
        builder.addOne(decodeOne(from, idx))
        idx += 1
      }

      Right(builder.result())
    } catch {
      case ex: ArrowDecoderError => Left(ex)
    }

  override def flatMap[B](f: Val => ArrowDecoder[Vector, B]): ArrowDecoder[Vector, B] =
    new ArrowVectorDecoder[Vector, B] {
      override protected[core] def decodeOne(from: Vector, idx: Int): B =
        f(self.decodeOne(from, idx)).decodeOne(from, idx)
    }

  override def map[B](f: Val => B): ArrowDecoder[Vector, B] =
    new ArrowVectorDecoder[Vector, B] {
      override protected[core] def decodeOne(from: Vector, idx: Int): B =
        f(self.decodeOne(from, idx))
    }

}

object ArrowVectorDecoder {

  def apply[Vector <: ValueVector, Val](implicit
    decoder: ArrowVectorDecoder[Vector, Val]
  ): ArrowVectorDecoder[Vector, Val] =
    decoder

  def apply[Vector <: ValueVector, Val](getIdx: Vector => Int => Val): ArrowVectorDecoder[Vector, Val] =
    new ArrowVectorDecoder[Vector, Val] {
      override protected[core] def decodeOne(from: Vector, idx: Int): Val =
        try getIdx(from)(idx)
        catch {
          case NonFatal(ex) => throw ArrowDecoderError("Error decoding vector", Some(ex))
        }
    }

  implicit val booleanDecoder: ArrowVectorDecoder[BitVector, Boolean]   =
    ArrowVectorDecoder(vec => idx => vec.getObject(idx))
  implicit val intDecoder: ArrowVectorDecoder[IntVector, Int]           =
    ArrowVectorDecoder(_.get)
  implicit val longDecoder: ArrowVectorDecoder[BigIntVector, Long]      =
    ArrowVectorDecoder(_.get)
  implicit val stringDecoder: ArrowVectorDecoder[VarCharVector, String] =
    ArrowVectorDecoder(vec => idx => new String(vec.get(idx), StandardCharsets.UTF_8))

  implicit def list[Val](implicit schema: Schema[Val]): ArrowVectorDecoder[ListVector, List[Val]] =
    ArrowVectorDecoder { vec => idx =>
      val reader = vec.getReader
      val buffer = ListBuffer.empty[Val]

      reader.setPosition(idx)
      while (reader.next())
        if (reader.isSet) {
          val dynamicValue = decodeSchema(None, schema, reader)

          dynamicValue.toTypedValue match {
            case Right(v)      => buffer.addOne(v)
            case Left(message) => throw ArrowDecoderError(message)
          }
        }

      buffer.result()
    }

  implicit def struct[Val](implicit schema: Schema[Val]): ArrowVectorDecoder[StructVector, Val] =
    ArrowVectorDecoder { vec => idx =>
      val dynamicValue = schema match {
        case record: Schema.Record[Val] =>
          val reader = vec.getReader

          reader.setPosition(idx)
          decodeCaseClass(record.fields, reader)
        case _                          =>
          throw ArrowDecoderError(s"Given ZIO schema must be of type Schema.Record[Val]")
      }

      dynamicValue.toTypedValue match {
        case Right(v)      => v
        case Left(message) => throw ArrowDecoderError(message)
      }
    }

  @tailrec
  private def decodeSchema[A](name: Option[String], schema0: Schema[A], reader0: FieldReader): DynamicValue =
    schema0 match {
      case record: Schema.Record[A]                =>
        val reader = name.fold[FieldReader](reader0.reader())(reader0.reader(_))
        decodeCaseClass(record.fields, reader)
      case Schema.Primitive(standardType, _)       =>
        val reader = name.fold[FieldReader](reader0.reader())(reader0.reader(_))
        decodePrimitive(standardType, reader)
      case Schema.Sequence(elemSchema, _, _, _, _) =>
        val reader = name.fold[FieldReader](reader0.reader())(reader0.reader(_))
        decodeSequence(elemSchema, reader)
      case lzy: Schema.Lazy[_]                     =>
        decodeSchema(name, lzy.schema, reader0)
      case other                                   =>
        throw ArrowDecoderError(s"Unsupported ZIO Schema type $other")
    }

  private def decodeCaseClass[A](fields: Chunk[Schema.Field[A, _]], reader0: FieldReader): DynamicValue = {
    val values = fields.map { case Schema.Field(name, schema0, _, _, _, _) =>
      val value: DynamicValue = decodeSchema(Some(name), schema0, reader0)

      name -> value
    }.to(ListMap)

    DynamicValue.Record(TypeId.Structural, values)
  }

  private def decodeSequence[A](schema0: Schema[A], reader0: FieldReader): DynamicValue = {
    val buffer = ListBuffer.empty[DynamicValue]

    while (reader0.next())
      if (reader0.isSet)
        buffer.addOne(decodeSchema(None, schema0, reader0))

    DynamicValue.Sequence(Chunk.fromIterable(buffer.result()))
  }

  private def decodePrimitive[A](standardType: StandardType[A], reader0: FieldReader): DynamicValue =
    standardType match {
      case t: StandardType.BoolType.type   =>
        DynamicValue.Primitive[Boolean](reader0.readBoolean(), t)
      case t: StandardType.IntType.type    =>
        DynamicValue.Primitive[Int](reader0.readInteger(), t)
      case t: StandardType.LongType.type   =>
        DynamicValue.Primitive[Long](reader0.readLong(), t)
      case t: StandardType.FloatType.type  =>
        DynamicValue.Primitive[Float](reader0.readFloat(), t)
      case t: StandardType.DoubleType.type =>
        DynamicValue.Primitive[Double](reader0.readDouble(), t)
      case t: StandardType.StringType.type =>
        DynamicValue.Primitive[String](reader0.readText().toString, t)
      case other                           =>
        throw ArrowDecoderError(s"Unsupported ZIO Schema type $other")
    }

}
