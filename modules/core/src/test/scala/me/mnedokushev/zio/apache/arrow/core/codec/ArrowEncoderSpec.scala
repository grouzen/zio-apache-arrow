package me.mnedokushev.zio.apache.arrow.core.codec

import me.mnedokushev.zio.apache.arrow.core.ZAllocator
import org.apache.arrow.vector.IntVector
import org.apache.arrow.vector.complex.ListVector
import zio._
import zio.test._
import zio.test.Assertion._

object ArrowEncoderSpec extends ZIOSpecDefault {

  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("ArrowEncoder")(
      vectorEncoderSpec
    ).provideLayer(ZAllocator.rootLayer())

  val vectorScalarEncoderSpec =
    suite("VectorScalarEncoder")(
      test("encode int") {
        ZIO.scoped(
          for {
            vec    <- VectorEncoder[Int, IntVector].encodeZIO(Chunk(1, 2, 3))
            result <- VectorDecoder[IntVector, Int].decodeZIO(vec)
          } yield assert(result)(equalTo(Chunk(1, 2, 3)))
        )
      },
      test("encode list int") {
        ZIO.scoped(
          for {
            vec    <- VectorEncoder[Set[Int], ListVector].encodeZIO(Chunk(Set(1, 2), Set(3)))
            result <- VectorDecoder[ListVector, List[Int]].decodeZIO(vec)
          } yield assert(result)(equalTo(Chunk(List(1, 2), List(3))))
        )
      }
    )

  val vectorEncoderSpec =
    suite("VectorEncoder")(
      vectorScalarEncoderSpec
    )

}
