package me.mnedokushev.zio.apache.arrow.core.codec

import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.{ ListVector, StructVector }
import org.apache.arrow.vector.complex.impl.{ BigIntReaderImpl, BitReaderImpl, IntReaderImpl }
import org.apache.arrow.vector.complex.reader.FieldReader
import org.apache.arrow.vector.types.pojo.ArrowType
import zio._
import zio.schema.{ DynamicValue, Schema, StandardType, TypeId }

import java.nio.charset.StandardCharsets
import scala.collection.immutable.ListMap
import scala.collection.mutable.ListBuffer
import scala.util.control.NonFatal

trait VectorDecoder[From <: ValueVector, +To] extends ArrowDecoder[From, To] { self =>

  override final def decode(from: From): Either[Throwable, Chunk[To]] =
    try {
      var idx        = 0
      val valueCount = from.getValueCount
      val builder    = ChunkBuilder.make[To](valueCount)

      while (idx < valueCount) {
        builder.addOne(decodeUnsafe(from, idx))
        idx += 1
      }

      Right(builder.result())
    } catch {
      case ex: DecoderError => Left(ex)
    }

  final def map[B](f: To => B): VectorDecoder[From, B] =
    new VectorDecoder[From, B] {
      override def decodeUnsafe(from: From, idx: Int): B =
        f(self.decodeUnsafe(from, idx))
    }

  final def flatMap[B](f: To => VectorDecoder[From, B]): VectorDecoder[From, B] =
    new VectorDecoder[From, B] {
      override def decodeUnsafe(from: From, idx: Int): B =
        f(self.decodeUnsafe(from, idx)).decodeUnsafe(from, idx)
    }

}

object VectorDecoder {

  def apply[From <: ValueVector, To](implicit vd: VectorDecoder[From, To]): VectorDecoder[From, To] =
    vd

  def apply[From <: ValueVector, To](getIdx: From => Int => To): VectorDecoder[From, To] =
    new VectorDecoder[From, To] {
      override def decodeUnsafe(from: From, idx: Int): To =
        try getIdx(from)(idx)
        catch {
          case NonFatal(ex) => throw DecoderError("Error decoding vector", Some(ex))
        }
    }

  implicit val booleanDecoder: VectorDecoder[BitVector, Boolean]   =
    VectorDecoder(vec => idx => vec.getObject(idx))
  implicit val intDecoder: VectorDecoder[IntVector, Int]           =
    VectorDecoder(_.get)
  implicit val longDecoder: VectorDecoder[BigIntVector, Long]      =
    VectorDecoder(_.get)
  implicit val stringDecoder: VectorDecoder[VarCharVector, String] =
    VectorDecoder(vec => idx => new String(vec.get(idx), StandardCharsets.UTF_8))

  implicit val listBooleanDecoder: VectorDecoder[ListVector, List[Boolean]] =
    listDecoder[Boolean, BitReaderImpl](_.readBoolean())
  implicit val listIntDecoder: VectorDecoder[ListVector, List[Int]]         =
    listDecoder[Int, IntReaderImpl](_.readInteger())
  implicit val listLongDecoder: VectorDecoder[ListVector, List[Long]]       =
    listDecoder[Long, BigIntReaderImpl](_.readLong())

  implicit def structDecoder[A](implicit schema: Schema[A]): VectorDecoder[StructVector, A] =
    VectorDecoder { vec => idx =>
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
                throw DecoderError(s"Unsupported Arrow type $other")
            }

            field.name.asInstanceOf[String] -> value
          }.to(ListMap)

          DynamicValue.Record(TypeId.Structural, listMap)
        case _                        =>
          throw DecoderError(s"Given ZIO schema must be of type Schema.Record[A]")
      }

      dynamicValue.toTypedValue match {
        case Right(v)      => v
        case Left(message) => throw DecoderError(message)
      }
    }

  private def listDecoder[Val, Reader](
    readVal: Reader => Val
  )(implicit ev: Reader <:< FieldReader): VectorDecoder[ListVector, List[Val]] =
    VectorDecoder { vec => idx =>
      val reader0    = vec.getReader
      val reader     = reader0.reader().asInstanceOf[Reader]
      val listBuffer = ListBuffer.empty[Val]

      reader0.setPosition(idx)
      while (reader0.next())
        if (reader.isSet)
          listBuffer.addOne(readVal(reader))

      listBuffer.result()
    }

}
