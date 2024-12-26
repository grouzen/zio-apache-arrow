package me.mnedokushev.zio.apache.arrow.core

import me.mnedokushev.zio.apache.arrow.core.codec.{ SchemaEncoder, VectorSchemaRootDecoder, VectorSchemaRootEncoder }
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.{ ArrowStreamReader, ArrowStreamWriter }
import zio._
import zio.schema.Schema
import zio.stream.ZStream

import java.io.{ ByteArrayOutputStream, InputStream }
import java.nio.channels.Channels

package object ipc {

  def readStreaming[A: Schema: SchemaEncoder](
    in: InputStream
  )(implicit
    decoder: VectorSchemaRootDecoder[A]
  ): ZStream[Scope & BufferAllocator, Throwable, A] =
    for {
      (reader, root) <- ZStream
                          .fromZIO(
                            ZIO.serviceWithZIO[BufferAllocator] { implicit alloc =>
                              for {
                                reader <- ZIO.fromAutoCloseable(ZIO.attempt(new ArrowStreamReader(in, alloc)))
                                root   <- ZIO.attemptBlockingIO(reader.getVectorSchemaRoot)
                                _      <- validateSchema(root.getSchema())
                              } yield (reader, root)
                            }
                          )
      chunk          <- ZStream.repeatZIOOption(
                          ZIO
                            .attemptBlockingIO(reader.loadNextBatch())
                            .asSomeError
                            .filterOrFail(_ == true)(None)
                            .zipRight(decoder.decodeZIO(root).asSomeError)
                        )
      elem           <- ZStream.fromIterable(chunk)
    } yield elem

  def writeStreaming[R, A: Schema: SchemaEncoder](
    in: ZStream[R, Throwable, A],
    // TODO: benchmark which value is more performant. See https://wesmckinney.com/blog/arrow-streaming-columnar/
    // TODO: ArrowBuf size is limited
    batchSize: Int = 2048
  )(implicit
    encoder: VectorSchemaRootEncoder[A]
  ): ZIO[R & Scope & BufferAllocator, Throwable, ByteArrayOutputStream] = {
    val out = new ByteArrayOutputStream()

    for {
      root   <- Tabular.empty[A]
      writer <- ZIO.fromAutoCloseable(ZIO.attempt(new ArrowStreamWriter(root, null, Channels.newChannel(out))))
      _      <- ZIO.attempt(writer.start())
      _      <- in.rechunk(batchSize).chunks.foreach { chunk =>
                  for {
                    _ <- encoder.encodeZIO(chunk, root)
                    _ <- ZIO.attempt(writer.writeBatch())
                  } yield ()
                }
    } yield out
  }

}
