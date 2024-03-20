package me.mnedokushev.zio.apache.arrow.datafusion

import me.mnedokushev.zio.apache.arrow.core._
import me.mnedokushev.zio.apache.arrow.core.codec.{ SchemaEncoder, VectorSchemaRootDecoder }
import org.apache.arrow.datafusion.DataFrame
import org.apache.arrow.memory.BufferAllocator
import zio._
import zio.schema.Schema
import zio.stream.ZStream

import java.nio.file.Path

class Dataframe(underlying: DataFrame) {

  def collect[A: Schema: SchemaEncoder](implicit
    decoder: VectorSchemaRootDecoder[A]
  ): ZStream[BufferAllocator, Throwable, A] =
    ZStream.serviceWithStream[BufferAllocator] { alloc =>
      for {
        reader <- ZStream.acquireReleaseWith(
                    ZIO.fromCompletableFuture(underlying.collect(alloc))
                  )(reader => ZIO.attempt(reader.close()).ignoreLogged)
        root   <- ZStream.fromZIO(
                    for {
                      root <- ZIO.attempt(reader.getVectorSchemaRoot)
                      _    <- validateSchema(root.getSchema())
                    } yield root
                  )
        chunk  <- ZStream.repeatZIOOption(
                    ZIO
                      .attempt(reader.loadNextBatch())
                      .asSomeError
                      .filterOrFail(_ == true)(None) *>
                      decoder.decodeZIO(root).asSomeError
                  )
        elem   <- ZStream.fromIterable(chunk)
      } yield elem
    }

  def show: Task[Unit] =
    ZIO.fromCompletableFuture(underlying.show()).unit

  def writeParquet(path: Path): Task[Unit] =
    ZIO.fromCompletableFuture(underlying.writeParquet(path)).unit

  def writeCsv(path: Path): Task[Unit] =
    ZIO.fromCompletableFuture(underlying.writeCsv(path)).unit

}
