package me.mnedokushev.zio.apache.arrow.core.codec

import me.mnedokushev.zio.apache.arrow.core.ArrowAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.ListVector
import zio._
import zio.test.Assertion._
import zio.test._

object ArrowCodecSpec extends ZIOSpecDefault {

  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("ArrowCodec")(
      vectorCodecSpec
    ).provideLayer(ArrowAllocator.rootLayer())

  val vectorCodecSpec =
    suite("ArrowVectorCodec")(
      test("decoder map") {
        ZIO.scoped(
          for {
            intVec <- ArrowVectorEncoder[Int, IntVector].encodeZIO(Chunk(1, 2, 3))
            result <- ArrowVectorDecoder[IntVector, Int].map(_.toString).decodeZIO(intVec)
          } yield assert(result)(equalTo(Chunk("1", "2", "3")))
        )
      },
      test("decoder flatMap") {
        val decoder = ArrowVectorDecoder[IntVector, Int]

        ZIO.scoped(
          for {
            intVec <- ArrowVectorEncoder[Int, IntVector].encodeZIO(Chunk(1, 2, 3))
            result <- decoder.flatMap {
                        case i if i % 2 == 0 =>
                          decoder.map(even => s"even:$even")
                        case _ =>
                          decoder.map(odd => s"odd:$odd")
                      }.decodeZIO(intVec)
          } yield assert(result)(equalTo(Chunk("odd:1", "even:2", "odd:3")))
        )
      },
      test("codec empty") {
        ZIO.scoped(
          for {
            scalarVec    <- ArrowVectorEncoder[Int, IntVector].encodeZIO(Chunk.empty)
            scalarResult <- ArrowVectorDecoder[IntVector, Int].decodeZIO(scalarVec)
            listVec      <- ArrowVectorEncoder[List[Int], ListVector].encodeZIO(Chunk.empty)
            listResult   <- ArrowVectorDecoder[ListVector, List[Int]].decodeZIO(listVec)
          } yield assertTrue(scalarResult.isEmpty) && assertTrue(listResult.isEmpty)
        )
      },
      test("codec boolean") {
        ZIO.scoped(
          for {
            vec    <- ArrowVectorEncoder[Boolean, BitVector].encodeZIO(Chunk(true, true, false))
            result <- ArrowVectorDecoder[BitVector, Boolean].decodeZIO(vec)
          } yield assert(result)(equalTo(Chunk(true, true, false)))
        )
      },
      test("codec int") {
        ZIO.scoped(
          for {
            vec    <- ArrowVectorEncoder[Int, IntVector].encodeZIO(Chunk(1, 2, 3))
            result <- ArrowVectorDecoder[IntVector, Int].decodeZIO(vec)
          } yield assert(result)(equalTo(Chunk(1, 2, 3)))
        )
      },
      test("codec long") {
        ZIO.scoped(
          for {
            vec    <- ArrowVectorEncoder[Long, BigIntVector].encodeZIO(Chunk(1L, 2L, 3L))
            result <- ArrowVectorDecoder[BigIntVector, Long].decodeZIO(vec)
          } yield assert(result)(equalTo(Chunk(1L, 2L, 3L)))
        )
      },
      test("codec string") {
        ZIO.scoped(
          for {
            vec    <- ArrowVectorEncoder[String, VarCharVector].encodeZIO(Chunk("zio", "cats", "monix"))
            result <- ArrowVectorDecoder[VarCharVector, String].decodeZIO(vec)
          } yield assert(result)(equalTo(Chunk("zio", "cats", "monix")))
        )
      },
      test("codec list boolean") {
        ZIO.scoped(
          for {
            vec    <- ArrowVectorEncoder[Set[Boolean], ListVector].encodeZIO(Chunk(Set(true), Set(false, true)))
            result <- ArrowVectorDecoder[ListVector, List[Boolean]].decodeZIO(vec)
          } yield assert(result)(equalTo(Chunk(List(true), List(false, true))))
        )
      },
      test("codec list int") {
        ZIO.scoped(
          for {
            vec    <- ArrowVectorEncoder[Set[Int], ListVector].encodeZIO(Chunk(Set(1, 2), Set(3)))
            result <- ArrowVectorDecoder[ListVector, List[Int]].decodeZIO(vec)
          } yield assert(result)(equalTo(Chunk(List(1, 2), List(3))))
        )
      },
      test("codec list long") {
        ZIO.scoped(
          for {
            vec    <- ArrowVectorEncoder[Set[Long], ListVector].encodeZIO(Chunk(Set(1L, 2L), Set(3L)))
            result <- ArrowVectorDecoder[ListVector, List[Long]].decodeZIO(vec)
          } yield assert(result)(equalTo(Chunk(List(1L, 2L), List(3L))))
        )
      }
    )

}
