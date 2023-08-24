package me.mnedokushev.zio.apache.arrow.core.codec

import me.mnedokushev.zio.apache.arrow.core.Fixtures._
import me.mnedokushev.zio.apache.arrow.core.{ Allocator, Tabular }
import org.apache.arrow.vector._
import zio._
import zio.test.Assertion._
import zio.test._
import org.apache.arrow.memory.BufferAllocator
import zio.test.Spec

object CodecSpec extends ZIOSpecDefault {

  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("Codec")(
      valueVectorCodecSpec,
      vectorSchemaRootCodecSpec
    ).provideLayerShared(Allocator.rootLayer())

  val valueVectorDecoderSpec: Spec[BufferAllocator, Throwable] =
    suite("ValueVectorDecoder")(
      test("map") {
        val codec = ValueVectorCodec[Int, IntVector]

        ZIO.scoped(
          for {
            intVec <- codec.encodeZIO(Chunk(1, 2, 3))
            result <- codec.decoder.map(_.toString).decodeZIO(intVec)
          } yield assert(result)(equalTo(Chunk("1", "2", "3")))
        )
      }
    )

  val valueVectorEncoderSpec: Spec[BufferAllocator, Throwable] =
    suite("ValueVectorEncoder")(
      test("contramap") {
        val codec = ValueVectorCodec[Int, IntVector]

        ZIO.scoped(
          for {
            intVec <- codec.encoder.contramap[String](s => s.toInt).encodeZIO(Chunk("1", "2", "3"))
            result <- codec.decodeZIO(intVec)
          } yield assert(result)(equalTo(Chunk(1, 2, 3)))
        )
      }
    )

  val valueVectorCodecPrimitiveSpec: Spec[BufferAllocator, Throwable] =
    suite("ValueVectorCodec primitive")(
      test("empty") {
        val codec = ValueVectorCodec[Int, IntVector]

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(Chunk.empty)
            result <- codec.decodeZIO(vec)
          } yield assertTrue(result.isEmpty)
        )
      },
      test("boolean") {
        val codec   = ValueVectorCodec[Boolean, BitVector]
        val payload = Chunk(true, true, false)

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("int") {
        val codec   = ValueVectorCodec[Int, IntVector]
        val payload = Chunk(1, 2, 3)

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("long") {
        val codec   = ValueVectorCodec[Long, BigIntVector]
        val payload = Chunk(1L, 2L, 3L)

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("string") {
        val codec   = ValueVectorCodec[String, VarCharVector]
        val payload = Chunk("zio", "cats", "monix")

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("optional string") {
        val codec = ValueVectorCodec[Option[String], VarCharVector]
        val payload = Chunk(Some("zio"), None, Some("monix"))

        ZIO.scoped(
          for {
            vec <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      }
    )

  val valueVectorCodecListSpec: Spec[BufferAllocator, Throwable] =
    suite("ValueVectorCodec list")(
      test("list empty") {
        val codec = ValueVectorCodec.list[Int]

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(Chunk.empty)
            result <- codec.decodeZIO(vec)
          } yield assertTrue(result.isEmpty)
        )
      },
      test("list boolean") {
        val codec   = ValueVectorCodec.list[Boolean]
        val payload = Chunk(Chunk(true), Chunk(false, true))

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("list int") {
        val codec   = ValueVectorCodec.list[Int]
        val payload = Chunk(Chunk(1, 2), Chunk(3))

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("list long") {
        val codec   = ValueVectorCodec.list[Long]
        val payload = Chunk(Chunk(1L, 2L), Chunk(3L))

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("list of primitives") {
        val codec   = ValueVectorCodec.list[List[Int]]
        val payload = Chunk(Chunk(List(1, 2), List(3)), Chunk(List(4), List(5, 6)))

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("list of structs") {
        val codec   = ValueVectorCodec.list[Primitives]
        val payload = Chunk(Chunk(Primitives(1, 2.0, "3"), Primitives(4, 5.0, "6")), Chunk(Primitives(7, 8.0, "9")))

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      }
    )

  val valueVectorCodecStructSpec: Spec[BufferAllocator, Throwable] =
    suite("ValueVectorCodec struct")(
      test("struct empty") {
        val codec   = ValueVectorCodec.struct[Primitives]
        val payload = Chunk.empty[Primitives]

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("struct primitives") {
        val codec   = ValueVectorCodec.struct[Primitives]
        val payload = Chunk(Primitives(1, 2.0, "3"))

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("struct of primitives") {
        val codec   = ValueVectorCodec.struct[StructOfPrimitives]
        val payload = Chunk(StructOfPrimitives(Primitives(1, 2.0, "3")))

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("struct of lists") {
        val codec   = ValueVectorCodec.struct[StructOfLists]
        val payload = Chunk(StructOfLists(ListOfPrimitives(List(1, 2, 3))))

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("struct of structs") {
        val codec   = ValueVectorCodec.struct[StructOfListsOfStructs]
        val payload = Chunk(
          StructOfListsOfStructs(ListOfStructs(List(Primitives(1, 2.0, "3"), Primitives(11, 22.0, "33"))))
        )

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("struct of lists of structs") {
        val codec   = ValueVectorCodec.struct[StructOfStructs]
        val payload = Chunk(StructOfStructs(StructOfPrimitives(Primitives(1, 2.0, "3"))))

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("struct of list of primitives") {
        val codec   = ValueVectorCodec.struct[ListOfPrimitives]
        val payload = Chunk(ListOfPrimitives(List(1, 2, 3)))

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("struct of list of structs") {
        val codec   = ValueVectorCodec.struct[ListOfStructs]
        val payload = Chunk(ListOfStructs(List(Primitives(1, 2.0, "3"), Primitives(11, 22.0, "33"))))

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("struct of list of lists") {
        val codec   = ValueVectorCodec.struct[ListOfLists]
        val payload = Chunk(ListOfLists(List(List(1, 2), List(3))))

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("struct of list of structs of lists") {
        val codec   = ValueVectorCodec.struct[ListOfStructsOfLists]
        val payload = Chunk(
          ListOfStructsOfLists(List(ListOfPrimitives(List(1, 2)), ListOfPrimitives(List(3)))),
          ListOfStructsOfLists(List(ListOfPrimitives(List(11, 22)), ListOfPrimitives(List(33))))
        )

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      }
    )

  val valueVectorCodecSpec: Spec[BufferAllocator, Throwable] =
    suite("ValueVectorCodec")(
      valueVectorDecoderSpec,
      valueVectorEncoderSpec,
      valueVectorCodecPrimitiveSpec,
      valueVectorCodecListSpec,
      valueVectorCodecStructSpec
    )

  val vectorSchemaRootDecoderSpec: Spec[BufferAllocator, Throwable] =
    suite("VectorSchemaRootDecoder")(
      test("map") {
        val codec = VectorSchemaRootCodec[Primitives]

        ZIO.scoped(
          for {
            root   <- Tabular.empty[Primitives]
            _      <- codec.encodeZIO(Chunk(Primitives(1, 2.0, "3")), root)
            result <- codec.decoder.map(p => s"${p.a}, ${p.b}, ${p.c}").decodeZIO(root)
          } yield assert(result)(equalTo(Chunk("1, 2.0, 3")))
        )
      }
    )

  val vectorSchemaRootEncoderSpec: Spec[BufferAllocator, Throwable] =
    suite("VectorSchemaRootEncoder")(
      test("contramap") {
        val codec = VectorSchemaRootCodec[Primitives]

        ZIO.scoped(
          for {
            root   <- Tabular.empty[Primitives]
            _      <- codec.encoder
                        .contramap[String](s => Primitives(s.toInt, s.toDouble, s))
                        .encodeZIO(Chunk("1", "2"), root)
            result <- codec.decodeZIO(root)
          } yield assert(result)(equalTo(Chunk(Primitives(1, 1.0, "1"), Primitives(2, 2.0, "2"))))
        )
      }
    )

  val vectorSchemaRootCodecSpec: Spec[BufferAllocator, Throwable] =
    suite("VectorSchemaRootCodec")(
      vectorSchemaRootDecoderSpec,
      vectorSchemaRootEncoderSpec,
      test("primitives") {
        val codec   = VectorSchemaRootCodec[Primitives]
        val payload = Chunk(Primitives(1, 2.0, "3"), Primitives(4, 5.0, "6"))

        ZIO.scoped(
          for {
            root   <- Tabular.empty[Primitives]
            _      <- codec.encodeZIO(payload, root)
            result <- codec.decodeZIO(root)
          } yield assert(result)(equalTo(payload))
        )
      }
    )

}
