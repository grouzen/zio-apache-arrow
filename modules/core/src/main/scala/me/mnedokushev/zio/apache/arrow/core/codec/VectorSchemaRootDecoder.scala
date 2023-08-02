package me.mnedokushev.zio.apache.arrow.core.codec

import org.apache.arrow.vector.VectorSchemaRoot
import zio._

import scala.util.control.NonFatal

trait VectorSchemaRootDecoder[+Val] extends Decoder[VectorSchemaRoot, Val] { self =>

  override final def decode(from: VectorSchemaRoot): Either[Throwable, Chunk[Val]] =
    try {
      var idx      = 0
      val rowCount = from.getRowCount
      val builder  = ChunkBuilder.make[Val](rowCount)

      while (idx <= rowCount) {
        builder.addOne(decodeUnsafe(from, idx))
        idx += 1
      }

      Right(builder.result())
    } catch {
      case ex: DecoderError => Left(ex)
    }

  protected def decodeUnsafe(from: VectorSchemaRoot, idx: Int): Val

  def flatMap[B](f: Val => VectorSchemaRootDecoder[B]): VectorSchemaRootDecoder[B] =
    new VectorSchemaRootDecoder[B] {
      override def decodeUnsafe(from: VectorSchemaRoot, idx: Int): B =
        f(self.decodeUnsafe(from, idx)).decodeUnsafe(from, idx)
    }

  def map[B](f: Val => B): VectorSchemaRootDecoder[B] =
    new VectorSchemaRootDecoder[B] {
      override def decodeUnsafe(from: VectorSchemaRoot, idx: Int): B =
        f(self.decodeUnsafe(from, idx))
    }

}

object VectorSchemaRootDecoder {

  def apply[To](getIdx: VectorSchemaRoot => Int => To): VectorSchemaRootDecoder[To] =
    new VectorSchemaRootDecoder[To] {
      override def decodeUnsafe(from: VectorSchemaRoot, idx: Int): To =
        try getIdx(from)(idx)
        catch {
          case NonFatal(ex) => throw DecoderError("Error decoding vector", Some(ex))
        }
    }

}
