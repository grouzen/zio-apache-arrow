package me.mnedokushev.zio.apache.arrow.core

import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.complex.impl.{ BigIntReaderImpl, BitReaderImpl, IntReaderImpl }
import org.apache.arrow.vector.complex.reader.FieldReader
import zio._

import java.nio.charset.StandardCharsets
import scala.collection.mutable.ListBuffer
import scala.util.control.NonFatal

trait VectorDecoder[-From <: ValueVector, +To] extends ArrowDecoder[From, To] { self =>

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

}

object VectorDecoder {

  def apply[From <: ValueVector, To](getIdx: From => Int => To): VectorDecoder[From, To] =
    new VectorDecoder[From, To] {
      override protected def decodeUnsafe(from: From, idx: Int): To =
        try getIdx(from)(idx)
        catch {
          case NonFatal(ex) => throw DecoderError("Error decoding vector", ex)
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
