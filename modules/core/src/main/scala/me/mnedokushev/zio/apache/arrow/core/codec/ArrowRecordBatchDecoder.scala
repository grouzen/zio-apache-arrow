package me.mnedokushev.zio.apache.arrow.core.codec

import org.apache.arrow.vector.VectorSchemaRoot
import zio._

import scala.util.control.NonFatal

trait ArrowRecordBatchDecoder[+Val] extends ArrowDecoder[VectorSchemaRoot, Val] { self =>

  override final def decode(from: VectorSchemaRoot): Either[Throwable, Chunk[Val]] =
    try {
      var idx      = 0
      val rowCount = from.getRowCount
      val builder  = ChunkBuilder.make[Val](rowCount)

      while (idx <= rowCount) {
        builder.addOne(decodeOne(from, idx))
        idx += 1
      }

      Right(builder.result())
    } catch {
      case ex: ArrowDecoderError => Left(ex)
    }

  override def flatMap[B](f: Val => ArrowDecoder[VectorSchemaRoot, B]): ArrowDecoder[VectorSchemaRoot, B] =
    new ArrowRecordBatchDecoder[B] {
      override def decodeOne(from: VectorSchemaRoot, idx: Int): B =
        f(self.decodeOne(from, idx)).decodeOne(from, idx)
    }

  override def map[B](f: Val => B): ArrowDecoder[VectorSchemaRoot, B] =
    new ArrowRecordBatchDecoder[B] {
      override def decodeOne(from: VectorSchemaRoot, idx: Int): B =
        f(self.decodeOne(from, idx))
    }

}

object ArrowRecordBatchDecoder {

  def apply[To](getIdx: VectorSchemaRoot => Int => To): ArrowRecordBatchDecoder[To] =
    new ArrowRecordBatchDecoder[To] {
      override def decodeOne(from: VectorSchemaRoot, idx: Int): To =
        try getIdx(from)(idx)
        catch {
          case NonFatal(ex) => throw ArrowDecoderError("Error decoding vector", Some(ex))
        }
    }

}
