package me.mnedokushev.zio.apache.arrow.core.codec

import me.mnedokushev.zio.apache.arrow.core.ArrowAllocator
import me.mnedokushev.zio.apache.arrow.core.codec.Fixtures._
import org.apache.arrow.vector._
import zio._
import zio.test.Assertion._
import zio.test._

object ArrowCodecSpec extends ZIOSpecDefault {

  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("ArrowCodec")(
      vectorCodecSpec
    ).provideLayerShared(ArrowAllocator.rootLayer())

  val vectorDecoderSpec =
    suite("ArrowVectorDecoder")(
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
      }
    )

  val vectorCodecPrimitiveSpec =
    suite("ArrowVector Encoder/Decoder primitive")(
      test("codec - empty") {
        ZIO.scoped(
          for {
            vec    <- ArrowVectorEncoder[Int, IntVector].encodeZIO(Chunk.empty)
            result <- ArrowVectorDecoder[IntVector, Int].decodeZIO(vec)
          } yield assertTrue(result.isEmpty)
        )
      },
      test("codec - boolean") {
        ZIO.scoped(
          for {
            vec    <- ArrowVectorEncoder[Boolean, BitVector].encodeZIO(Chunk(true, true, false))
            result <- ArrowVectorDecoder[BitVector, Boolean].decodeZIO(vec)
          } yield assert(result)(equalTo(Chunk(true, true, false)))
        )
      },
      test("codec - int") {
        ZIO.scoped(
          for {
            vec    <- ArrowVectorEncoder[Int, IntVector].encodeZIO(Chunk(1, 2, 3))
            result <- ArrowVectorDecoder[IntVector, Int].decodeZIO(vec)
          } yield assert(result)(equalTo(Chunk(1, 2, 3)))
        )
      },
      test("codec - long") {
        ZIO.scoped(
          for {
            vec    <- ArrowVectorEncoder[Long, BigIntVector].encodeZIO(Chunk(1L, 2L, 3L))
            result <- ArrowVectorDecoder[BigIntVector, Long].decodeZIO(vec)
          } yield assert(result)(equalTo(Chunk(1L, 2L, 3L)))
        )
      },
      test("codec - string") {
        ZIO.scoped(
          for {
            vec    <- ArrowVectorEncoder[String, VarCharVector].encodeZIO(Chunk("zio", "cats", "monix"))
            result <- ArrowVectorDecoder[VarCharVector, String].decodeZIO(vec)
          } yield assert(result)(equalTo(Chunk("zio", "cats", "monix")))
        )
      }
    )

  val vectorCodecListSpec =
    suite("ArrowVector Encoder/Decoder list")(
      test("codec list - empty") {
        ZIO.scoped(
          for {
            vec    <- ArrowVectorEncoder.list[Int, List].encodeZIO(Chunk.empty)
            result <- ArrowVectorDecoder.list[Int].decodeZIO(vec)
          } yield assertTrue(result.isEmpty)
        )
      },
      test("codec list - boolean") {
        ZIO.scoped(
          for {
            vec    <- ArrowVectorEncoder.list[Boolean, Set].encodeZIO(Chunk(Set(true), Set(false, true)))
            result <- ArrowVectorDecoder.list[Boolean].decodeZIO(vec)
          } yield assert(result)(equalTo(Chunk(List(true), List(false, true))))
        )
      },
      test("codec list - int") {
        ZIO.scoped(
          for {
            vec    <- ArrowVectorEncoder.list[Int, Set].encodeZIO(Chunk(Set(1, 2), Set(3)))
            result <- ArrowVectorDecoder.list[Int].decodeZIO(vec)
          } yield assert(result)(equalTo(Chunk(List(1, 2), List(3))))
        )
      },
      test("codec list - long") {
        ZIO.scoped(
          for {
            vec    <- ArrowVectorEncoder.list[Long, Set].encodeZIO(Chunk(Set(1L, 2L), Set(3L)))
            result <- ArrowVectorDecoder.list[Long].decodeZIO(vec)
          } yield assert(result)(equalTo(Chunk(List(1L, 2L), List(3L))))
        )
      },
      test("codec list - list of primitives") {
        ZIO.scoped(
          for {
            vec    <- ArrowVectorEncoder
                        .list[List[Int], List]
                        .encodeZIO(Chunk(List(List(1, 2), List(3)), List(List(4), List(5, 6))))
            result <- ArrowVectorDecoder.list[List[Int]].decodeZIO(vec)
          } yield assert(result)(equalTo(Chunk(List(List(1, 2), List(3)), List(List(4), List(5, 6)))))
        )
      },
      test("codec list - list of structs") {
        ZIO.scoped(
          for {
            vec    <-
              ArrowVectorEncoder
                .list[Primitives, List]
                .encodeZIO(Chunk(List(Primitives(1, 2.0, "3"), Primitives(4, 5.0, "6")), List(Primitives(7, 8.0, "9"))))
            result <- ArrowVectorDecoder.list[Primitives].decodeZIO(vec)
          } yield assert(result)(
            equalTo(Chunk(List(Primitives(1, 2.0, "3"), Primitives(4, 5.0, "6")), List(Primitives(7, 8.0, "9"))))
          )
        )
      }
    )

  val vectorCodecStructSpec =
    suite("ArrowVector Encoder/Decoder struct")(
      test("codec struct - empty") {
        val payload = Chunk.empty[Primitives]

        ZIO.scoped(
          for {
            vec    <- ArrowVectorEncoder.struct[Primitives].encodeZIO(payload)
            result <- ArrowVectorDecoder.struct[Primitives].decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("codec struct - primitives") {
        val payload = Chunk(Primitives(1, 2.0, "3"))

        ZIO.scoped(
          for {
            vec    <- ArrowVectorEncoder.struct[Primitives].encodeZIO(payload)
            result <- ArrowVectorDecoder.struct[Primitives].decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("codec struct - struct of primitives") {
        val payload = Chunk(StructOfPrimitives(Primitives(1, 2.0, "3")))

        ZIO.scoped(
          for {
            vec    <- ArrowVectorEncoder.struct[StructOfPrimitives].encodeZIO(payload)
            result <- ArrowVectorDecoder.struct[StructOfPrimitives].decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("codec struct - struct of lists") {
        val payload = Chunk(StructOfLists(ListOfPrimitives(List(1, 2, 3))))

        ZIO.scoped(
          for {
            vec    <- ArrowVectorEncoder.struct[StructOfLists].encodeZIO(payload)
            result <- ArrowVectorDecoder.struct[StructOfLists].decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("codec struct - struct of structs") {
        val payload =
          Chunk(StructOfListsOfStructs(ListOfStructs(List(Primitives(1, 2.0, "3"), Primitives(11, 22.0, "33")))))

        ZIO.scoped(
          for {
            vec    <- ArrowVectorEncoder.struct[StructOfListsOfStructs].encodeZIO(payload)
            result <- ArrowVectorDecoder.struct[StructOfListsOfStructs].decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("codec struct - struct of lists of structs") {
        val payload = Chunk(StructOfStructs(StructOfPrimitives(Primitives(1, 2.0, "3"))))

        ZIO.scoped(
          for {
            vec    <- ArrowVectorEncoder.struct[StructOfStructs].encodeZIO(payload)
            result <- ArrowVectorDecoder.struct[StructOfStructs].decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("codec struct - list of primitives") {
        val payload = Chunk(ListOfPrimitives(List(1, 2, 3)))

        ZIO.scoped(
          for {
            vec    <- ArrowVectorEncoder.struct[ListOfPrimitives].encodeZIO(payload)
            result <- ArrowVectorDecoder.struct[ListOfPrimitives].decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("codec struct - list of structs") {
        val payload = Chunk(ListOfStructs(List(Primitives(1, 2.0, "3"), Primitives(11, 22.0, "33"))))

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
          ListOfStructsOfLists(List(ListOfPrimitives(List(1, 2)), ListOfPrimitives(List(3)))),
          ListOfStructsOfLists(List(ListOfPrimitives(List(11, 22)), ListOfPrimitives(List(33))))
        )

        ZIO.scoped(
          for {
            vec    <- ArrowVectorEncoder.struct[ListOfStructsOfLists].encodeZIO(payload)
            result <- ArrowVectorDecoder.struct[ListOfStructsOfLists].decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      }
    )

  val vectorCodecSpec =
    suite("ArrowVector Encoder/Decoder")(
      vectorDecoderSpec,
      vectorCodecPrimitiveSpec,
      vectorCodecListSpec,
      vectorCodecStructSpec
    )

}
