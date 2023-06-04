package me.mnedokushev.zio.apache.arrow.core.codec

import me.mnedokushev.zio.apache.arrow.core.ArrowAllocator
import me.mnedokushev.zio.apache.arrow.core.codec.Fixtures._
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.ListVector
import zio._
import zio.schema._
import zio.test.Assertion._
import zio.test._

object ArrowCodecSpec extends ZIOSpecDefault {

  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("ArrowCodec")(
      vectorCodecSpec
    ).provideLayerShared(ArrowAllocator.rootLayer())

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
            vec    <- ArrowVectorEncoder[Int, IntVector].encodeZIO(Chunk.empty)
            result <- ArrowVectorDecoder[IntVector, Int].decodeZIO(vec)
          } yield assertTrue(result.isEmpty)
        )
      },
      suite("scalar")(
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
        }
      ),
      suite("list")(
        test("codec list empty") {
          ZIO.scoped(
            for {
              vec    <- ArrowVectorEncoder[List[Int], ListVector].encodeZIO(Chunk.empty)
              result <- ArrowVectorDecoder[ListVector, List[Int]].decodeZIO(vec)
            } yield assertTrue(result.isEmpty)
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
      ),
      suite("struct")(
        test("codec struct - empty") {
          val payload = Chunk.empty[Scalars]

          ZIO.scoped(
            for {
              vec    <- ArrowVectorEncoder.struct[Scalars].encodeZIO(payload)
              result <- ArrowVectorDecoder.struct[Scalars].decodeZIO(vec)
            } yield assert(result)(equalTo(payload))
          )
        },
        test("codec struct - scalars") {
          val payload = Chunk(Scalars(1, 2.0, "3"))

          ZIO.scoped(
            for {
              vec    <- ArrowVectorEncoder.struct[Scalars].encodeZIO(payload)
              result <- ArrowVectorDecoder.struct[Scalars].decodeZIO(vec)
            } yield assert(result)(equalTo(payload))
          )
        },
        test("codec struct - struct of scalars") {
          val payload = Chunk(StructOfScalars(Scalars(1, 2.0, "3")))

          ZIO.scoped(
            for {
              vec    <- ArrowVectorEncoder.struct[StructOfScalars].encodeZIO(payload)
              result <- ArrowVectorDecoder.struct[StructOfScalars].decodeZIO(vec)
            } yield assert(result)(equalTo(payload))
          )
        },
        test("codec struct - struct of lists") {
          val payload = Chunk(StructOfLists(ListOfScalars(List(1, 2, 3))))

          ZIO.scoped(
            for {
              vec    <- ArrowVectorEncoder.struct[StructOfLists].encodeZIO(payload)
              result <- ArrowVectorDecoder.struct[StructOfLists].decodeZIO(vec)
            } yield assert(result)(equalTo(payload))
          )
        },
        test("codec struct - struct of structs") {
          val payload =
            Chunk(StructOfListsOfStructs(ListOfStructs(List(Scalars(1, 2.0, "3"), Scalars(11, 22.0, "33")))))

          ZIO.scoped(
            for {
              vec    <- ArrowVectorEncoder.struct[StructOfListsOfStructs].encodeZIO(payload)
              result <- ArrowVectorDecoder.struct[StructOfListsOfStructs].decodeZIO(vec)
            } yield assert(result)(equalTo(payload))
          )
        },
        test("codec struct - struct of lists of structs") {
          val payload = Chunk(StructOfStructs(StructOfScalars(Scalars(1, 2.0, "3"))))

          ZIO.scoped(
            for {
              vec    <- ArrowVectorEncoder.struct[StructOfStructs].encodeZIO(payload)
              result <- ArrowVectorDecoder.struct[StructOfStructs].decodeZIO(vec)
            } yield assert(result)(equalTo(payload))
          )
        },
        test("codec struct - list of scalars") {
          val payload = Chunk(ListOfScalars(List(1, 2, 3)))

          ZIO.scoped(
            for {
              vec    <- ArrowVectorEncoder.struct[ListOfScalars].encodeZIO(payload)
              result <- ArrowVectorDecoder.struct[ListOfScalars].decodeZIO(vec)
            } yield assert(result)(equalTo(payload))
          )
        },
        test("codec struct - list of structs") {
          val payload = Chunk(ListOfStructs(List(Scalars(1, 2.0, "3"), Scalars(11, 22.0, "33"))))

          ZIO.scoped(
            for {
              vec    <- ArrowVectorEncoder.struct[ListOfStructs].encodeZIO(payload)
              result <- ArrowVectorDecoder.struct[ListOfStructs].decodeZIO(vec)
            } yield assert(result)(equalTo(payload))
          )
        },
        test("codec struct - list of lists") {
          val payload = Chunk(ListOfLists(List(List(1, 2), List(3))))

          ZIO.scoped(
            for {
              vec    <- ArrowVectorEncoder.struct[ListOfLists].encodeZIO(payload)
              result <- ArrowVectorDecoder.struct[ListOfLists].decodeZIO(vec)
            } yield assert(result)(equalTo(payload))
          )
        },
        test("codec struct - list of structs of lists") {
          val payload = Chunk(
            ListOfStructsOfLists(List(ListOfScalars(List(1, 2)), ListOfScalars(List(3)))),
            ListOfStructsOfLists(List(ListOfScalars(List(11, 22)), ListOfScalars(List(33))))
          )

          ZIO.scoped(
            for {
              vec    <- ArrowVectorEncoder.struct[ListOfStructsOfLists].encodeZIO(payload)
              result <- ArrowVectorDecoder.struct[ListOfStructsOfLists].decodeZIO(vec)
            } yield assert(result)(equalTo(payload))
          )
        }
      )
    )

}
