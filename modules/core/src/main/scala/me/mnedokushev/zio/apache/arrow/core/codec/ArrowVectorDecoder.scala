package me.mnedokushev.zio.apache.arrow.core.codec

import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.{ ListVector, StructVector }
import org.apache.arrow.vector.complex.impl._
import org.apache.arrow.vector.complex.reader.FieldReader
import org.apache.arrow.vector.types.pojo.ArrowType
import zio._
import zio.schema._

import java.nio.charset.StandardCharsets
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
      override def decodeOne(from: Vector, idx: Int): B =
        f(self.decodeOne(from, idx)).decodeOne(from, idx)
    }

  override def map[B](f: Val => B): ArrowDecoder[Vector, B] =
    new ArrowVectorDecoder[Vector, B] {
      override def decodeOne(from: Vector, idx: Int): B =
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
      override def decodeOne(from: Vector, idx: Int): Val =
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

  implicit val listBooleanDecoder: ArrowVectorDecoder[ListVector, List[Boolean]] =
    list[Boolean, BitReaderImpl](_.readBoolean())
  implicit val listIntDecoder: ArrowVectorDecoder[ListVector, List[Int]]         =
    list[Int, IntReaderImpl](_.readInteger())
  implicit val listLongDecoder: ArrowVectorDecoder[ListVector, List[Long]]       =
    list[Long, BigIntReaderImpl](_.readLong())

  private def list[Val, Reader](
    readVal: Reader => Val
  )(implicit ev: Reader <:< FieldReader): ArrowVectorDecoder[ListVector, List[Val]] =
    ArrowVectorDecoder { vec => idx =>
      val reader0    = vec.getReader
      val reader     = reader0.reader().asInstanceOf[Reader]
      val listBuffer = ListBuffer.empty[Val]

      reader0.setPosition(idx)
      while (reader0.next())
        if (reader.isSet)
          listBuffer.addOne(readVal(reader))

      listBuffer.result()
    }

  implicit def struct[A](implicit schema: Schema[A]): ArrowVectorDecoder[StructVector, A] =
    ArrowVectorDecoder { vec => idx =>
      /*
        1. transform StructVector to DynamicValue
           - read value by idx for each field
           - create DynamicValue from the list map of fieldName => value
        2. materialize DynamicValue to value (literally case class) with validation
           - map ZIO Schema DecodeError to ZIO Apache Arrow DecoderError
       */
      val reader0 = vec.getReader

      reader0.setPosition(idx)
      val dynamicValue = schema match {
        case record: Schema.Record[A] =>
          val listMap = record.fields.map { field =>
            val reader = reader0.reader(field.name)

            val value: DynamicValue = reader0.getField.getType match {
              case _: ArrowType.Int  =>
                DynamicValue.Primitive[Int](reader.readInteger(), StandardType.IntType)
              case _: ArrowType.Bool =>
                DynamicValue.Primitive[Boolean](reader.readBoolean(), StandardType.BoolType)
              case _: ArrowType.List => ??? // recursion
              case other             =>
                throw ArrowDecoderError(s"Unsupported Arrow type $other")
            }

            field.name.asInstanceOf[String] -> value
          }.to(ListMap)

          DynamicValue.Record(TypeId.Structural, listMap)
        case _                        =>
          throw ArrowDecoderError(s"Given ZIO schema must be of type Schema.Record[A]")
      }

      dynamicValue.toTypedValue match {
        case Right(v)      => v
        case Left(message) => throw ArrowDecoderError(message)
      }
    }

}
