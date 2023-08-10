package me.mnedokushev.zio.apache.arrow.core.codec

import me.mnedokushev.zio.apache.arrow.core.{ Allocator, Tabular }
import me.mnedokushev.zio.apache.arrow.core.codec.Fixtures._
import org.apache.arrow.vector._
import zio._
import zio.test.Assertion._
import zio.test._

object CodecSpec extends ZIOSpecDefault {

  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("Codec")(
      valueVectorCodecSpec,
      vectorSchemaRootCodecSpec
    ).provideLayerShared(Allocator.rootLayer())

//  val vectorDecoderSpec =
//    suite("ArrowVectorDecoder")(
//      test("decoder map") {
//        ZIO.scoped(
//          for {
//            intVec <- ArrowVectorEncoder.primitive[Int, IntVector].encodeZIO(Chunk(1, 2, 3))
//            result <- ArrowVectorDecoder.primitive[IntVector, Int].map(_.toString).decodeZIO(intVec)
//          } yield assert(result)(equalTo(Chunk("1", "2", "3")))
//        )
//      },
//      test("decoder flatMap") {
//        val decoder = ArrowVectorDecoder.primitive[IntVector, Int]
//
//        ZIO.scoped(
//          for {
//            intVec <- ArrowVectorEncoder.primitive[Int, IntVector].encodeZIO(Chunk(1, 2, 3))
//            result <- decoder.flatMap {
//                        case i if i % 2 == 0 =>
//                          decoder.map(even => s"even:$even")
//                        case _ =>
//                          decoder.map(odd => s"odd:$odd")
//                      }.decodeZIO(intVec)
//          } yield assert(result)(equalTo(Chunk("odd:1", "even:2", "odd:3")))
//        )
//      }
//    )

  val valueVectorCodecPrimitiveSpec =
    suite("ValueVector Encoder/Decoder primitive")(
      test("codec - empty") {
        ZIO.scoped(
          for {
            vec    <- ValueVectorEncoder.primitive[Int, IntVector].encodeZIO(Chunk.empty)
            result <- ValueVectorDecoder.primitive[IntVector, Int].decodeZIO(vec)
          } yield assertTrue(result.isEmpty)
        )
      },
      test("codec - boolean") {
        ZIO.scoped(
          for {
            vec    <- ValueVectorEncoder.primitive[Boolean, BitVector].encodeZIO(Chunk(true, true, false))
            result <- ValueVectorDecoder.primitive[BitVector, Boolean].decodeZIO(vec)
          } yield assert(result)(equalTo(Chunk(true, true, false)))
        )
      },
      test("codec - int") {
        ZIO.scoped(
          for {
            vec    <- ValueVectorEncoder.primitive[Int, IntVector].encodeZIO(Chunk(1, 2, 3))
            result <- ValueVectorDecoder.primitive[IntVector, Int].decodeZIO(vec)
          } yield assert(result)(equalTo(Chunk(1, 2, 3)))
        )
      },
      test("codec - long") {
        ZIO.scoped(
          for {
            vec    <- ValueVectorEncoder.primitive[Long, BigIntVector].encodeZIO(Chunk(1L, 2L, 3L))
            result <- ValueVectorDecoder.primitive[BigIntVector, Long].decodeZIO(vec)
          } yield assert(result)(equalTo(Chunk(1L, 2L, 3L)))
        )
      },
      test("codec - string") {
        ZIO.scoped(
          for {
            vec    <- ValueVectorEncoder.primitive[String, VarCharVector].encodeZIO(Chunk("zio", "cats", "monix"))
            result <- ValueVectorDecoder.primitive[VarCharVector, String].decodeZIO(vec)
          } yield assert(result)(equalTo(Chunk("zio", "cats", "monix")))
        )
      }
    )

  val valueVectorCodecListSpec =
    suite("ValueVector Encoder/Decoder list")(
      test("codec list - empty") {
        ZIO.scoped(
          for {
            vec    <- ValueVectorEncoder.list[Int, List].encodeZIO(Chunk.empty)
            result <- ValueVectorDecoder.list[Int, List].decodeZIO(vec)
          } yield assertTrue(result.isEmpty)
        )
      },
      test("codec list - boolean") {
        ZIO.scoped(
          for {
            vec    <- ValueVectorEncoder.list[Boolean, Set].encodeZIO(Chunk(Set(true), Set(false, true)))
            result <- ValueVectorDecoder.list[Boolean, List].decodeZIO(vec)
          } yield assert(result)(equalTo(Chunk(List(true), List(false, true))))
        )
      },
      test("codec list - int") {
        ZIO.scoped(
          for {
            vec    <- ValueVectorEncoder.list[Int, Set].encodeZIO(Chunk(Set(1, 2), Set(3)))
            result <- ValueVectorDecoder.list[Int, List].decodeZIO(vec)
          } yield assert(result)(equalTo(Chunk(List(1, 2), List(3))))
        )
      },
      test("codec list - long") {
        ZIO.scoped(
          for {
            vec    <- ValueVectorEncoder.list[Long, Set].encodeZIO(Chunk(Set(1L, 2L), Set(3L)))
            result <- ValueVectorDecoder.list[Long, List].decodeZIO(vec)
          } yield assert(result)(equalTo(Chunk(List(1L, 2L), List(3L))))
        )
      },
      test("codec list - list of primitives") {
        ZIO.scoped(
          for {
            vec    <- ValueVectorEncoder
                        .list[List[Int], List]
                        .encodeZIO(Chunk(List(List(1, 2), List(3)), List(List(4), List(5, 6))))
            result <- ValueVectorDecoder.list[List[Int], List].decodeZIO(vec)
          } yield assert(result)(equalTo(Chunk(List(List(1, 2), List(3)), List(List(4), List(5, 6)))))
        )
      },
      test("codec list - list of structs") {
        ZIO.scoped(
          for {
            vec    <-
              ValueVectorEncoder
                .list[Primitives, List]
                .encodeZIO(Chunk(List(Primitives(1, 2.0, "3"), Primitives(4, 5.0, "6")), List(Primitives(7, 8.0, "9"))))
            result <- ValueVectorDecoder.list[Primitives, List].decodeZIO(vec)
          } yield assert(result)(
            equalTo(Chunk(List(Primitives(1, 2.0, "3"), Primitives(4, 5.0, "6")), List(Primitives(7, 8.0, "9"))))
          )
        )
      }
    )

  val valueVectorCodecStructSpec =
    suite("ValueVector Encoder/Decoder struct")(
      test("codec struct - empty") {
        val payload = Chunk.empty[Primitives]

        ZIO.scoped(
          for {
            vec    <- ValueVectorEncoder.struct[Primitives].encodeZIO(payload)
            result <- ValueVectorDecoder.struct[Primitives].decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("codec struct - primitives") {
        val payload = Chunk(Primitives(1, 2.0, "3"))

        ZIO.scoped(
          for {
            vec    <- ValueVectorEncoder.struct[Primitives].encodeZIO(payload)
            result <- ValueVectorDecoder.struct[Primitives].decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("codec struct - struct of primitives") {
        val payload = Chunk(StructOfPrimitives(Primitives(1, 2.0, "3")))

        ZIO.scoped(
          for {
            vec    <- ValueVectorEncoder.struct[StructOfPrimitives].encodeZIO(payload)
            result <- ValueVectorDecoder.struct[StructOfPrimitives].decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("codec struct - struct of lists") {
        val payload = Chunk(StructOfLists(ListOfPrimitives(List(1, 2, 3))))

        ZIO.scoped(
          for {
            vec    <- ValueVectorEncoder.struct[StructOfLists].encodeZIO(payload)
            result <- ValueVectorDecoder.struct[StructOfLists].decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("codec struct - struct of structs") {
        val payload =
          Chunk(StructOfListsOfStructs(ListOfStructs(List(Primitives(1, 2.0, "3"), Primitives(11, 22.0, "33")))))

        ZIO.scoped(
          for {
            vec    <- ValueVectorEncoder.struct[StructOfListsOfStructs].encodeZIO(payload)
            result <- ValueVectorDecoder.struct[StructOfListsOfStructs].decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("codec struct - struct of lists of structs") {
        val payload = Chunk(StructOfStructs(StructOfPrimitives(Primitives(1, 2.0, "3"))))

        ZIO.scoped(
          for {
            vec    <- ValueVectorEncoder.struct[StructOfStructs].encodeZIO(payload)
            result <- ValueVectorDecoder.struct[StructOfStructs].decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("codec struct - list of primitives") {
        val payload = Chunk(ListOfPrimitives(List(1, 2, 3)))

        ZIO.scoped(
          for {
            vec    <- ValueVectorEncoder.struct[ListOfPrimitives].encodeZIO(payload)
            result <- ValueVectorDecoder.struct[ListOfPrimitives].decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("codec struct - list of structs") {
        val payload = Chunk(ListOfStructs(List(Primitives(1, 2.0, "3"), Primitives(11, 22.0, "33"))))

        ZIO.scoped(
          for {
            vec    <- ValueVectorEncoder.struct[ListOfStructs].encodeZIO(payload)
            result <- ValueVectorDecoder.struct[ListOfStructs].decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("codec struct - list of lists") {
        val payload = Chunk(ListOfLists(List(List(1, 2), List(3))))

        ZIO.scoped(
          for {
            vec    <- ValueVectorEncoder.struct[ListOfLists].encodeZIO(payload)
            result <- ValueVectorDecoder.struct[ListOfLists].decodeZIO(vec)
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
            vec    <- ValueVectorEncoder.struct[ListOfStructsOfLists].encodeZIO(payload)
            result <- ValueVectorDecoder.struct[ListOfStructsOfLists].decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      }
    )

  val vectorSchemaRootCodecSpec =
    suite("VectorSchemaRoot Encoder/Decoder")(
      test("codec - primitives") {
        ZIO.scoped(
          for {
            rootVec <- Tabular.empty[Primitives]
            _       <- VectorSchemaRootEncoder
                         .schema[Primitives]
                         .encodeZIO(Chunk(Primitives(1, 2.0, "3"), Primitives(4, 5.0, "6")), rootVec)
            result  <- VectorSchemaRootDecoder.schema[Primitives].decodeZIO(rootVec)
          } yield assert(result)(equalTo(Chunk(Primitives(1, 2.0, "3"), Primitives(4, 5.0, "6"))))
        )
      }
    )

  val valueVectorCodecSpec =
    suite("ValueVector Encoder/Decoder")(
//      vectorDecoderSpec,
      valueVectorCodecPrimitiveSpec,
      valueVectorCodecListSpec,
      valueVectorCodecStructSpec
    )

}
