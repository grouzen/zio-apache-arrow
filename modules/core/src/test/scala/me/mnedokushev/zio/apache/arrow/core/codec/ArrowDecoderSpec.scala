package me.mnedokushev.zio.apache.arrow.core.codec

import me.mnedokushev.zio.apache.arrow.core.ZAllocator
import me.mnedokushev.zio.apache.arrow.core.vector.ZVector
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.ListVector
import zio._
import zio.test.Assertion._
import zio.test._

object ArrowDecoderSpec extends ZIOSpecDefault {

  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("ArrowDecoder")(
      vectorDecoderSpec
    ).provideLayer(ZAllocator.rootLayer())

  val vectorDecoderSpec =
    suite("VectorDecoder")(
      test("map") {
        ZIO.scoped(
          for {
            intVec <- ZVector.Int(1, 2, 3)
            result <- VectorDecoder.intDecoder.map(_.toString).decodeZIO(intVec)
          } yield assert(result)(equalTo(Chunk("1", "2", "3")))
        )
      },
      test("flatMap") {
        ZIO.scoped(
          for {
            intVec <- ZVector.Int(1, 2, 3)
            result <- VectorDecoder.intDecoder.flatMap {
                        case i if i % 2 == 0 =>
                          VectorDecoder.intDecoder.map(even => s"even:$even")
                        case _ =>
                          VectorDecoder.intDecoder.map(odd => s"odd:$odd")
                      }.decodeZIO(intVec)
          } yield assert(result)(equalTo(Chunk("odd:1", "even:2", "odd:3")))
        )
      },
      test("empty") {
        ZIO.scoped(
          for {
            intVec     <- ZVector.Int.empty
            listIntVec <- ZVector.ListInt.empty

            intResult     <- VectorDecoder[IntVector, Int].decodeZIO(intVec)
            listIntResult <- VectorDecoder[ListVector, List[Int]].decodeZIO(listIntVec)
          } yield assertTrue(intResult.isEmpty) && assertTrue(listIntResult.isEmpty)
        )
      },
      test("decode boolean") {
        ZIO.scoped(
          for {
            vec    <- ZVector.Boolean(true, true, false)
            result <- VectorDecoder[BitVector, Boolean].decodeZIO(vec)
          } yield assert(result)(equalTo(Chunk(true, true, false)))
        )
      },
      test("decode int") {
        ZIO.scoped(
          for {
            vec    <- ZVector.Int(1, 2, 3)
            result <- VectorDecoder[IntVector, Int].decodeZIO(vec)
          } yield assert(result)(equalTo(Chunk(1, 2, 3)))
        )
      },
      test("decode long") {
        ZIO.scoped(
          for {
            vec    <- ZVector.Long(1, 2, 3)
            result <- VectorDecoder[BigIntVector, Long].decodeZIO(vec)
          } yield assert(result)(equalTo(Chunk(1L, 2L, 3L)))
        )
      },
      test("decode string") {
        ZIO.scoped(
          for {
            vec    <- ZVector.String("zio", "cats", "monix")
            result <- VectorDecoder[VarCharVector, String].decodeZIO(vec)
          } yield assert(result)(equalTo(Chunk("zio", "cats", "monix")))
        )
      },
      test("decode list int") {
        ZIO.scoped(
          for {
            vec    <- ZVector.ListInt(List(1, 2), List(3))
            result <- VectorDecoder[ListVector, List[Int]].decodeZIO(vec)
          } yield assert(result)(equalTo(Chunk(List(1, 2), List(3))))
        )
      }
    )
}
