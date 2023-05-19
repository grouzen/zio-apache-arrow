package me.mnedokushev.zio.apache.arrow.core

import org.apache.arrow.vector._
import zio._
import zio.test._
import zio.test.Assertion._

object ArrowDecoderSpec extends ZIOSpecDefault {

  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("ArrowDecoder")(
      vectorDecoderSpec
    )

  val vectorDecoderSpec =
    suite("VectorDecoder")(
      test("empty") {
        for {
          vec    <- ZVector.Int.empty
          result <- VectorDecoder[IntVector, Int].decodeZio(vec)
        } yield assertTrue(result.isEmpty)
      },
      test("decode boolean") {
        for {
          vec    <- ZVector.Boolean(true, true, false)
          result <- VectorDecoder[BitVector, Boolean].decodeZio(vec)
        } yield assert(result)(equalTo(Chunk(true, true, false)))
      },
      test("decode int") {
        for {
          vec    <- ZVector.Int(1, 2, 3)
          result <- VectorDecoder[IntVector, Int].decodeZio(vec)
        } yield assert(result)(equalTo(Chunk(1, 2, 3)))
      },
      test("decode long") {
        for {
          vec    <- ZVector.Long(1, 2, 3)
          result <- VectorDecoder[BigIntVector, Long].decodeZio(vec)
        } yield assert(result)(equalTo(Chunk(1L, 2L, 3L)))
      },
      test("decode string") {
        for {
          vec    <- ZVector.String("zio", "cats", "monix")
          result <- VectorDecoder[VarCharVector, String].decodeZio(vec)
        } yield assert(result)(equalTo(Chunk("zio", "cats", "monix")))
      }
    ).provideLayer(ZAllocator.rootLayer())
}
