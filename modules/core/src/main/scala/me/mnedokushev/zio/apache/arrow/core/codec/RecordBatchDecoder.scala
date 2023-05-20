package me.mnedokushev.zio.apache.arrow.core.codec

import org.apache.arrow.vector.VectorSchemaRoot
import zio._

import scala.util.control.NonFatal

trait RecordBatchDecoder[+To] extends ArrowDecoder[VectorSchemaRoot, To] { self =>

  override final def decode(from: VectorSchemaRoot): Either[Throwable, Chunk[To]] =
    try {
      var idx      = 0
      val rowCount = from.getRowCount
      val builder  = ChunkBuilder.make[To](rowCount)

      while (idx <= rowCount) {
        builder.addOne(decodeUnsafe(from, idx))
        idx += 1
      }

      Right(builder.result())
    } catch {
      case ex: DecoderError => Left(ex)
    }

}

object RecordBatchDecoder {

  def apply[To](getIdx: VectorSchemaRoot => Int => To): RecordBatchDecoder[To] =
    new RecordBatchDecoder[To] {
      override protected def decodeUnsafe(from: VectorSchemaRoot, idx: RuntimeFlags): To =
        try getIdx(from)(idx)
        catch {
          case NonFatal(ex) => throw DecoderError("Error decoding vector", ex)
        }
    }

}
