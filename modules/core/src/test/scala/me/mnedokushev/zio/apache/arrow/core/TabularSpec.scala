package me.mnedokushev.zio.apache.arrow.core

import me.mnedokushev.zio.apache.arrow.core.Fixtures._
import me.mnedokushev.zio.apache.arrow.core.codec.VectorSchemaRootDecoder
import zio._
import zio.stream.ZStream
import zio.test.Assertion._
import zio.test._

object TabularSpec extends ZIOSpecDefault {

  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("Tabular")(
      test("empty") {
        ZIO.scoped(
          for {
            root   <- Tabular.empty[NullablePrimitives]
            result <- VectorSchemaRootDecoder[NullablePrimitives].decodeZIO(root)
          } yield assert(result)(isEmpty)
        )
      },
      test("fromChunk") {
        val payload = Chunk(Primitives(1, 1.0, "1"), Primitives(2, 2.0, "2"))

        ZIO.scoped(
          for {
            root   <- Tabular.fromChunk(payload)
            result <- VectorSchemaRootDecoder[Primitives].decodeZIO(root)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("fromStream") {
        val payload = Chunk(Primitives(1, 1.0, "1"), Primitives(2, 2.0, "2"))

        ZIO.scoped(
          for {
            root   <- Tabular.fromStream(ZStream.fromChunk(payload))
            result <- VectorSchemaRootDecoder[Primitives].decodeZIO(root)
          } yield assert(result)(equalTo(payload))
        )
      }
    ).provideLayerShared(Allocator.rootLayer())

}
