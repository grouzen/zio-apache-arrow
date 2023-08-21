package me.mnedokushev.zio.apache.arrow.core

import me.mnedokushev.zio.apache.arrow.core.codec.ValueVectorDecoder
import org.apache.arrow.vector.IntVector
import zio._
import zio.stream.ZStream
import zio.test.Assertion._
import zio.test._

object VectorSpec extends ZIOSpecDefault {

  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("Vector")(
      test("fromChunk") {
        val payload = Chunk(1, 2, 3)

        ZIO.scoped(
          for {
            vec    <- Vector.fromChunk[IntVector](payload)
            result <- ValueVectorDecoder.primitive[IntVector, Int].decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("fromStream") {
        val payload = Chunk(1, 2, 3)

        ZIO.scoped(
          for {
            vec    <- Vector.fromStream[IntVector](ZStream.from(payload))
            result <- ValueVectorDecoder.primitive[IntVector, Int].decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("toChunk") {
        val payload          = Chunk(1, 2, 3)
        // TODO: figure out how to avoid this
        implicit val decoder = ValueVectorDecoder.primitive[IntVector, Int]

        ZIO.scoped(
          for {
            vec    <- Vector.fromChunk[IntVector](payload)
            result <- Vector.toChunk[Int](vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("toStream") {
        val payload          = Chunk(1, 2, 3)
        // TODO: figure out how to avoid this
        implicit val decoder = ValueVectorDecoder.primitive[IntVector, Int]

        ZIO.scoped(
          for {
            vec    <- Vector.fromChunk[IntVector](payload)
            result <- Vector.toStream[Int](vec).runCollect
          } yield assert(result)(equalTo(payload))
        )
      }
    ).provideLayerShared(Allocator.rootLayer())

}
