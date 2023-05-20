package me.mnedokushev.zio.apache.arrow.core.vector

import me.mnedokushev.zio.apache.arrow.core.ZAllocator
import me.mnedokushev.zio.apache.arrow.core.vector.ZVector.ListInt
import zio._
import zio.test._
import zio.test.Assertion._

object ZVectorSpec extends ZIOSpecDefault {

  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("ZVector")(
      vectorListSpec
    ).provideLayer(ZAllocator.rootLayer())

  val vectorListSpec =
    suite("ZVectorList")(
      test("fromChunk of Lists") {
        val chunk = Chunk(List(1, 2), List(3))

        ZIO.scoped(
          for {
            listIntVec <- ListInt.fromChunk(chunk)
            result     <- ListInt.decodeZIO(listIntVec)
          } yield assert(result)(equalTo(chunk))
        )
      },
      test("fromChunk of Chunks") {
        val chunk = Chunk(Chunk(1), Chunk(2, 3))

        ZIO.scoped(
          for {
            listIntVec <- ListInt.fromChunk(chunk)
            result     <- ListInt.decodeZIO(listIntVec)
          } yield assert(result)(equalTo(Chunk(List(1), List(2, 3))))
        )
      },
      test("fromChunk of Sets") {
        val chunk = Chunk(Set(1, 2), Set(3))

        ZIO.scoped(
          for {
            listIntVec <- ListInt.fromChunk(chunk)
            result     <- ListInt.decodeZIO(listIntVec)
          } yield assert(result)(equalTo(Chunk(List(1, 2), List(3))))
        )
      }
    )

}
