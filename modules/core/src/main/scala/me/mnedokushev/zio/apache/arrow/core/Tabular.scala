package me.mnedokushev.zio.apache.arrow.core

import me.mnedokushev.zio.apache.arrow.core.codec.SchemaEncoder
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import zio._
import zio.schema.{ Schema => ZSchema }
import scala.jdk.CollectionConverters._
import org.apache.arrow.vector.types.pojo.Schema

object Tabular {

  def empty[Val](implicit schema: ZSchema[Val]): RIO[Scope with BufferAllocator, VectorSchemaRoot] =
    ZIO.fromAutoCloseable(
      ZIO.serviceWithZIO[BufferAllocator] { implicit alloc =>
        for {
          schema0 <- ZIO.fromEither(SchemaEncoder.schemaRoot[Val])
          vectors <- ZIO.foreach(schema0.getFields.asScala.toList) { f =>
                       for {
                         vec <- ZIO.attempt(f.createVector(alloc))
                         _   <- ZIO.attempt(vec.allocateNew())
                       } yield vec
                     }
          vec     <- ZIO.attempt(new VectorSchemaRoot(schema0.getFields, vectors.asJava))
        } yield vec
      }
    )

}
