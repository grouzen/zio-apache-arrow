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
      test("fromChunk") {
        val chunk = Chunk(List(1, 2), List(3))

        ZIO.scoped(
          for {
            listIntVec <- ListInt.fromChunk(chunk)
            result     <- ListInt.decodeZIO(listIntVec)
          } yield assert(chunk)(equalTo(result))
        )
      }
    )

}
