package me.mnedokushev.zio.apache.arrow.core

import me.mnedokushev.zio.apache.arrow.core.codec._
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import zio._
import zio.schema.{ Schema => ZSchema }
import zio.stream.ZStream

import scala.jdk.CollectionConverters._

object Tabular {

  def empty[A: ZSchema](implicit
    schemaEncoder: SchemaEncoder[A]
  ): RIO[Scope with BufferAllocator, VectorSchemaRoot] =
    ZIO.fromAutoCloseable(
      ZIO.serviceWithZIO[BufferAllocator] { implicit alloc =>
        for {
          schema0 <- ZIO.fromEither(schemaEncoder.encode)
          vectors <- ZIO.foreach(schema0.getFields.asScala.toList) { f =>
                       for {
                         vec <- ZIO.attempt(f.createVector(alloc))
                         _   <- ZIO.attempt(vec.allocateNew())
                       } yield vec
                     }
          root    <- ZIO.attempt(new VectorSchemaRoot(schema0.getFields, vectors.asJava))
        } yield root
      }
    )

  def fromChunk[A: ZSchema: SchemaEncoder](chunk: Chunk[A])(implicit
    encoder: VectorSchemaRootEncoder[A]
  ): RIO[Scope with BufferAllocator, VectorSchemaRoot] =
    for {
      root <- empty
      _    <- encoder.encodeZIO(chunk, root)
    } yield root

  def fromStream[R, A: ZSchema: SchemaEncoder](stream: ZStream[R, Throwable, A])(implicit
    encoder: VectorSchemaRootEncoder[A]
  ): RIO[R with Scope with BufferAllocator, VectorSchemaRoot] =
    for {
      chunk <- stream.runCollect
      root  <- fromChunk(chunk)
    } yield root

  def toChunk[A](root: VectorSchemaRoot)(implicit decoder: VectorSchemaRootDecoder[A]): Task[Chunk[A]] =
    decoder.decodeZIO(root)

  def toStream[A](root: VectorSchemaRoot)(implicit decoder: VectorSchemaRootDecoder[A]): ZStream[Any, Throwable, A] =
    ZStream.fromIterableZIO(decoder.decodeZIO(root))

}
