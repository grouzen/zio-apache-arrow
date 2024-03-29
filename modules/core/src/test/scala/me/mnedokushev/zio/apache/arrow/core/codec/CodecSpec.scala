package me.mnedokushev.zio.apache.arrow.core.codec

import me.mnedokushev.zio.apache.arrow.core.Fixtures._
import me.mnedokushev.zio.apache.arrow.core.{ Allocator, Tabular }
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import zio._
import zio.test.Assertion._
import zio.test.{ Spec, _ }

import java.time.{
  DayOfWeek,
  Instant,
  LocalDate,
  LocalDateTime,
  LocalTime,
  Month,
  MonthDay,
  OffsetDateTime,
  OffsetTime,
  Period,
  Year,
  YearMonth,
  ZoneId,
  ZoneOffset,
  ZonedDateTime
}
import java.util.UUID

object CodecSpec extends ZIOSpecDefault {

  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("Codec")(
      valueVectorCodecSpec,
      vectorSchemaRootCodecSpec
    ).provideLayerShared(Allocator.rootLayer())

  val valueVectorDecoderSpec: Spec[BufferAllocator, Throwable] =
    suite("ValueVectorDecoder")(
      test("map") {
        val codec = ValueVectorCodec.primitive[Int, IntVector]

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
        val codec = ValueVectorCodec.primitive[Int, IntVector]

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
        val codec = ValueVectorCodec.primitive[Int, IntVector]

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(Chunk.empty)
            result <- codec.decodeZIO(vec)
          } yield assertTrue(result.isEmpty)
        )
      },
      test("string") {
        val codec   = ValueVectorCodec.primitive[String, VarCharVector]
        val payload = Chunk("zio", "cats", "monix")

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("boolean") {
        val codec   = ValueVectorCodec.primitive[Boolean, BitVector]
        val payload = Chunk(true, true, false)

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("byte") {
        val codec   = ValueVectorCodec.primitive[Byte, UInt1Vector]
        val payload = Chunk[Byte](1, 2, 3)

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("short") {
        val codec   = ValueVectorCodec.primitive[Short, SmallIntVector]
        val payload = Chunk[Short](1, 2, 3)

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("int") {
        val codec   = ValueVectorCodec.primitive[Int, IntVector]
        val payload = Chunk(1, 2, 3)

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("long") {
        val codec   = ValueVectorCodec.primitive[Long, BigIntVector]
        val payload = Chunk(1L, 2L, 3L)

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("float") {
        val codec   = ValueVectorCodec.primitive[Float, Float4Vector]
        val payload = Chunk(1.0f, 2.0f, 3.0f)

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("double") {
        val codec   = ValueVectorCodec.primitive[Double, Float8Vector]
        val payload = Chunk(1.0, 2.0, 3.0)

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("binary") {
        // TODO: avoid specifying schema explicitly in this case
        val codec                       =
          ValueVectorCodec.primitive[Chunk[Byte], LargeVarBinaryVector](zio.schema.Schema.primitive[Chunk[Byte]])
        val payload: Chunk[Chunk[Byte]] = Chunk(Chunk(1, 2, 3), Chunk(4, 5, 6))

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("char") {
        val codec   = ValueVectorCodec.primitive[Char, UInt2Vector]
        val payload = Chunk('a', 'b', 'c')

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("uuid") {
        val codec   = ValueVectorCodec.primitive[UUID, VarBinaryVector]
        val payload = Chunk(UUID.randomUUID(), UUID.randomUUID())

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("big decimal") {
        val codec   = ValueVectorCodec.primitive[java.math.BigDecimal, DecimalVector]
        val payload = Chunk(new java.math.BigDecimal("12312.33"), new java.math.BigDecimal("9990221.33"))

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("big integer") {
        val codec   = ValueVectorCodec.primitive[java.math.BigInteger, VarBinaryVector]
        val payload = Chunk(new java.math.BigInteger("1231233999"), new java.math.BigInteger("9990221001223"))

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("day of week") {
        val codec   = ValueVectorCodec.primitive[DayOfWeek, IntVector]
        val payload = Chunk(DayOfWeek.of(1), DayOfWeek.of(2))

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("month") {
        val codec   = ValueVectorCodec.primitive[Month, IntVector]
        val payload = Chunk(Month.of(1), Month.of(2))

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("month day") {
        val codec   = ValueVectorCodec.primitive[MonthDay, BigIntVector]
        val payload = Chunk(MonthDay.of(1, 31), MonthDay.of(2, 28))

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("period") {
        val codec   = ValueVectorCodec.primitive[Period, VarBinaryVector]
        val payload = Chunk(Period.of(1970, 1, 31), Period.of(1990, 2, 28))

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("year") {
        val codec   = ValueVectorCodec.primitive[Year, IntVector]
        val payload = Chunk(Year.of(1970), Year.of(1989))

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("year month") {
        val codec   = ValueVectorCodec.primitive[YearMonth, BigIntVector]
        val payload = Chunk(YearMonth.of(1970, 1), YearMonth.of(1989, 5))

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("zone id") {
        val codec   = ValueVectorCodec.primitive[ZoneId, VarCharVector]
        val payload = Chunk(ZoneId.of("Australia/Sydney"), ZoneId.of("Africa/Harare"))

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("zone offset") {
        val codec   = ValueVectorCodec.primitive[ZoneOffset, VarCharVector]
        val payload = Chunk(ZoneOffset.of("+1"), ZoneOffset.of("+3"))

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("duration") {
        val codec   = ValueVectorCodec.primitive[Duration, BigIntVector]
        val payload = Chunk(Duration.fromMillis(0), Duration.fromMillis(10092))

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("instant") {
        val codec   = ValueVectorCodec.primitive[Instant, BigIntVector]
        val payload = Chunk(Instant.ofEpochMilli(123), Instant.ofEpochMilli(999312))

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("local date") {
        val codec   = ValueVectorCodec.primitive[LocalDate, VarCharVector]
        val payload = Chunk(LocalDate.of(1970, 1, 23), LocalDate.of(1989, 5, 31))

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("local time") {
        val codec   = ValueVectorCodec.primitive[LocalTime, VarCharVector]
        val payload = Chunk(LocalTime.of(3, 12), LocalTime.of(5, 15))

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("local date time") {
        val codec   = ValueVectorCodec.primitive[LocalDateTime, VarCharVector]
        val payload = Chunk(LocalDateTime.of(1970, 1, 3, 3, 12), LocalDateTime.of(1989, 5, 31, 5, 15))

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("offset time") {
        val codec   = ValueVectorCodec.primitive[OffsetTime, VarCharVector]
        val payload = Chunk(
          OffsetTime.of(LocalTime.of(3, 12), ZoneOffset.of("+1")),
          OffsetTime.of(LocalTime.of(4, 12), ZoneOffset.of("-2"))
        )

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("offset date time") {
        val codec   = ValueVectorCodec.primitive[OffsetDateTime, VarCharVector]
        val payload = Chunk(
          OffsetDateTime.of(LocalDate.of(1970, 1, 3), LocalTime.of(3, 12), ZoneOffset.of("+1")),
          OffsetDateTime.of(LocalDate.of(1989, 5, 31), LocalTime.of(4, 12), ZoneOffset.of("-2"))
        )

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("zoned date time") {
        val codec   = ValueVectorCodec.primitive[ZonedDateTime, VarCharVector]
        val payload = Chunk(
          ZonedDateTime.of(LocalDateTime.of(1970, 1, 3, 3, 12), ZoneId.of("Asia/Dhaka")),
          ZonedDateTime.of(LocalDateTime.of(1989, 5, 31, 3, 12), ZoneId.of("Asia/Shanghai"))
        )

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      }
      //      test("optional string") {
//        val codec = ValueVectorCodec[Option[String], VarCharVector]
//        val payload = Chunk(Some("zio"), None, Some("monix"))
//
//        ZIO.scoped(
//          for {
//            vec <- codec.encodeZIO(payload)
//            result <- codec.decodeZIO(vec)
//          } yield assert(result)(equalTo(payload))
//        )
//      }
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
      test("list string") {
        val codec   = ValueVectorCodec.list[String]
        val payload = Chunk(Chunk("zio"), Chunk("cats", "monix"))

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("lit boolean") {
        val codec   = ValueVectorCodec.list[Boolean]
        val payload = Chunk(Chunk(true), Chunk(true, false))

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("list byte") {
        val codec                       = ValueVectorCodec.list[Byte]
        val payload: Chunk[Chunk[Byte]] = Chunk(Chunk(1), Chunk(2, 3))

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("list short") {
        val codec                        = ValueVectorCodec.list[Short]
        val payload: Chunk[Chunk[Short]] = Chunk(Chunk(1), Chunk(2, 3))

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
      test("list float") {
        val codec   = ValueVectorCodec.list[Float]
        val payload = Chunk(Chunk(1.0f, 2.0f), Chunk(3.0f))

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("list double") {
        val codec   = ValueVectorCodec.list[Double]
        val payload = Chunk(Chunk(1.0, 2.0), Chunk(3.0))

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("list binary") {
        val codec                              = ValueVectorCodec.list[Chunk[Byte]]
        val payload: Chunk[Chunk[Chunk[Byte]]] = Chunk(Chunk(Chunk(1, 2, 3)), Chunk(Chunk(4, 5, 6)))

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("list char") {
        val codec   = ValueVectorCodec.list[Char]
        val payload = Chunk(Chunk('a'), Chunk('b', 'c'))

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      // TODO: fix encoder
//      test("list uuid") {
      //        val codec   = ValueVectorCodec.list[UUID]
      //        val payload = Chunk(Chunk(UUID.randomUUID()), Chunk(UUID.randomUUID()))
      //
      //        ZIO.scoped(
      //          for {
      //            vec    <- codec.encodeZIO(payload)
      //            result <- codec.decodeZIO(vec)
      //          } yield assert(result)(equalTo(payload))
      //        )
      //      },
      test("list big decimal") {
        val codec   = ValueVectorCodec.list[java.math.BigDecimal]
        val payload = Chunk(Chunk(new java.math.BigDecimal("12312.33")), Chunk(new java.math.BigDecimal("9990221.33")))

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("list big integer") {
        val codec   = ValueVectorCodec.list[java.math.BigInteger]
        val payload =
          Chunk(Chunk(new java.math.BigInteger("1231233999")), Chunk(new java.math.BigInteger("9990221001223")))

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("list day of week") {
        val codec   = ValueVectorCodec.list[DayOfWeek]
        val payload = Chunk(Chunk(DayOfWeek.of(1)), Chunk(DayOfWeek.of(2)))

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("list month") {
        val codec   = ValueVectorCodec.list[Month]
        val payload = Chunk(Chunk(Month.of(1)), Chunk(Month.of(2)))

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("list month day") {
        val codec   = ValueVectorCodec.list[MonthDay]
        val payload = Chunk(Chunk(MonthDay.of(1, 31)), Chunk(MonthDay.of(2, 28)))

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      // TODO: fix encoder
//      test("list period") {
//        val codec   = ValueVectorCodec.list[Period]
//        val payload = Chunk(Chunk(Period.of(1970, 1, 31)), Chunk(Period.of(1990, 2, 28)))
//
//        ZIO.scoped(
//          for {
//            vec    <- codec.encodeZIO(payload)
//            result <- codec.decodeZIO(vec)
//          } yield assert(result)(equalTo(payload))
//        )
//      },
      test("list year") {
        val codec   = ValueVectorCodec.list[Year]
        val payload = Chunk(Chunk(Year.of(1970)), Chunk(Year.of(2001)))

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("list year month") {
        val codec   = ValueVectorCodec.list[YearMonth]
        val payload = Chunk(Chunk(YearMonth.of(1970, 3)), Chunk(YearMonth.of(2001, 2)))

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("list zone id") {
        val codec   = ValueVectorCodec.list[ZoneId]
        val payload = Chunk(Chunk(ZoneId.of("America/St_Johns")), Chunk(ZoneId.of("Europe/Paris")))

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("list zone offset") {
        val codec   = ValueVectorCodec.list[ZoneOffset]
        val payload = Chunk(Chunk(ZoneOffset.of("+1")), Chunk(ZoneOffset.of("+2")))

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("list duration") {
        val codec   = ValueVectorCodec.list[Duration]
        val payload = Chunk(Chunk(Duration.fromMillis(0)), Chunk(Duration.fromMillis(1000)))

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("list instant") {
        val codec   = ValueVectorCodec.list[Instant]
        val payload = Chunk(Chunk(Instant.ofEpochMilli(0)), Chunk(Instant.ofEpochMilli(1000)))

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("list local date") {
        val codec   = ValueVectorCodec.list[LocalDate]
        val payload = Chunk(Chunk(LocalDate.of(1970, 1, 1)), Chunk(LocalDate.of(1980, 2, 3)))

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("list local time") {
        val codec   = ValueVectorCodec.list[LocalTime]
        val payload = Chunk(Chunk(LocalTime.of(13, 1)), Chunk(LocalTime.of(16, 15)))

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("list local date time") {
        val codec   = ValueVectorCodec.list[LocalDateTime]
        val payload = Chunk(Chunk(LocalDateTime.of(1970, 1, 23, 13, 1)), Chunk(LocalDateTime.of(1989, 5, 31, 2, 15)))

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("list offset time") {
        val codec   = ValueVectorCodec.list[OffsetTime]
        val payload = Chunk(
          Chunk(OffsetTime.of(LocalTime.of(13, 1), ZoneOffset.of("+1"))),
          Chunk(OffsetTime.of(LocalTime.of(16, 15), ZoneOffset.of("+3")))
        )

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("list offset date time") {
        val codec   = ValueVectorCodec.list[OffsetDateTime]
        val payload = Chunk(
          Chunk(OffsetDateTime.of(LocalDate.of(1970, 1, 1), LocalTime.of(13, 1), ZoneOffset.of("+1"))),
          Chunk(OffsetDateTime.of(LocalDate.of(1989, 5, 31), LocalTime.of(16, 15), ZoneOffset.of("+3")))
        )

        ZIO.scoped(
          for {
            vec    <- codec.encodeZIO(payload)
            result <- codec.decodeZIO(vec)
          } yield assert(result)(equalTo(payload))
        )
      },
      test("list zoned date time") {
        val codec   = ValueVectorCodec.list[ZonedDateTime]
        val payload = Chunk(
          Chunk(ZonedDateTime.of(LocalDateTime.of(1970, 1, 23, 13, 1), ZoneId.of("Asia/Tokyo"))),
          Chunk(ZonedDateTime.of(LocalDateTime.of(1989, 5, 31, 8, 30), ZoneId.of("Africa/Cairo")))
        )

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
