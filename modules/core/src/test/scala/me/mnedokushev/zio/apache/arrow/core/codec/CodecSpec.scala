package me.mnedokushev.zio.apache.arrow.core.codec

// import me.mnedokushev.zio.apache.arrow.core.Fixtures._
// import me.mnedokushev.zio.apache.arrow.core.{ Allocator, Tabular }
import me.mnedokushev.zio.apache.arrow.core.Fixtures._
import me.mnedokushev.zio.apache.arrow.core.{ Allocator, Tabular }
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.complex.StructVector
import zio._
import zio.schema._
import zio.schema.Schema._
import zio.schema.Factory._
import zio.test.Assertion._
import zio.test.{ Spec, _ }
import me.mnedokushev.zio.apache.arrow.core.codec.ValueVectorEncoder._
import me.mnedokushev.zio.apache.arrow.core.codec.ValueVectorDecoder._
import org.apache.arrow.vector._
// import java.util.UUID

object CodecSpec extends ZIOSpecDefault {

  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("Codec")(
      valueVectorDecoderSpec,
      valueVectorEncoderSpec,
      valueVectorCodecSpec,
      vectorSchemaRootCodecSpec
    ).provideLayerShared(Allocator.rootLayer())

  val valueVectorDecoderSpec: Spec[BufferAllocator, Throwable] =
    suite("ValueVectorDecoder")(
      test("map") {
        import ValueVectorCodec._

        ZIO.scoped(
          for {
            intVec <- intCodec.encodeZIO(Chunk(1, 2, 3))
            result <- intCodec.decoder.map(_.toString).decodeZIO(intVec)
          } yield assert(result)(equalTo(Chunk("1", "2", "3")))
        )
      }
    )

  val valueVectorEncoderSpec: Spec[BufferAllocator, Throwable] =
    suite("ValueVectorEncoder")(
      test("contramap") {
        import ValueVectorCodec._

        ZIO.scoped(
          for {
            intVec <- intCodec.encoder.contramap[String](s => s.toInt).encodeZIO(Chunk("1", "2", "3"))
            result <- intCodec.decodeZIO(intVec)
          } yield assert(result)(equalTo(Chunk(1, 2, 3)))
        )
      }
    )

  val valueVectorCodecSpec: Spec[BufferAllocator, Throwable] =
    suite("ValueVectorCodec")(
      test("primitive") {
        import ValueVectorCodec._

        val emptyPayload                      = Chunk[Int]()
        val stringPaylod                      = Chunk("zio", "cats", "monix")
        val boolPayload                       = Chunk(true, false)
        val bytePayload                       = Chunk[Byte](1, 2)
        val shortPayload                      = Chunk[Short](1, 2)
        val intPayload                        = Chunk(1, 2, 3)
        val longPayload                       = Chunk(1L, 2L, 3L)
        val floatPayload                      = Chunk(0.5f, 1.5f, 2.5f)
        val doublePayload                     = Chunk(0.5d, 1.5d, 2.5d)
        val binaryPayload: Chunk[Chunk[Byte]] = Chunk(Chunk(1, 2, 3), Chunk(4, 5, 6))
        val charPayload                       = Chunk('a', 'b')
        // val uuidPayload                       = Chunk(UUID.randomUUID(), UUID.randomUUID())
        val bigDecimalPayload                 = Chunk(new java.math.BigDecimal("12312.33"), new java.math.BigDecimal("9990221.33"))
        val bigIntegerPayload                 = Chunk(new java.math.BigInteger("1231233999"), new java.math.BigInteger("9990221001223"))
        val dayOfWeekPayload                  = Chunk(java.time.DayOfWeek.MONDAY, java.time.DayOfWeek.TUESDAY)
        val monthPayload                      = Chunk(java.time.Month.JANUARY, java.time.Month.FEBRUARY)
        val monthDayPayload                   =
          Chunk(java.time.MonthDay.of(java.time.Month.JANUARY, 1), java.time.MonthDay.of(java.time.Month.FEBRUARY, 2))
        // val periodPayload                     = Chunk(java.time.Period.ofDays(1), java.time.Period.ofMonths(2))
        val yearPayload                       = Chunk(java.time.Year.of(2019), java.time.Year.of(2020))
        val yearMonthPayload                  = Chunk(java.time.YearMonth.of(2019, 3), java.time.YearMonth.of(2020, 4))
        // val zoneIdPayload                     = Chunk(java.time.ZoneId.of("Australia/Sydney"), java.time.ZoneId.of("Africa/Harare"))
        val zoneOffsetPayload                 = Chunk(java.time.ZoneOffset.of("+1"), java.time.ZoneOffset.of("+3"))
        val durationPayload                   = Chunk(java.time.Duration.ofDays(2), java.time.Duration.ofHours(4))
        val instantPayload                    = Chunk(java.time.Instant.ofEpochMilli(123), java.time.Instant.ofEpochMilli(999312))
        val localDatePayload                  = Chunk(java.time.LocalDate.of(2018, 5, 7), java.time.LocalDate.of(2019, 6, 4))
        val localTimePayload                  = Chunk(java.time.LocalTime.of(12, 30), java.time.LocalTime.of(8, 45))
        val localDateTimePayload              =
          Chunk(java.time.LocalDateTime.of(2018, 3, 12, 12, 30), java.time.LocalDateTime.of(2019, 4, 5, 7, 45))
        val offsetTimePayload                 =
          Chunk(
            java.time.OffsetTime.of(java.time.LocalTime.of(3, 12), java.time.ZoneOffset.of("+1")),
            java.time.OffsetTime.of(java.time.LocalTime.of(4, 12), java.time.ZoneOffset.of("-2"))
          )
        val offsetDateTimePayload             =
          Chunk(
            java.time.OffsetDateTime
              .of(java.time.LocalDate.of(1970, 1, 3), java.time.LocalTime.of(3, 12), java.time.ZoneOffset.of("+1")),
            java.time.OffsetDateTime
              .of(java.time.LocalDate.of(1989, 5, 31), java.time.LocalTime.of(4, 12), java.time.ZoneOffset.of("-2"))
          )
        val zonedDateTimePayload              =
          Chunk(
            java.time.ZonedDateTime
              .of(java.time.LocalDateTime.of(1970, 1, 3, 3, 12), java.time.ZoneId.of("Asia/Dhaka")),
            java.time.ZonedDateTime
              .of(java.time.LocalDateTime.of(1989, 5, 31, 3, 12), java.time.ZoneId.of("Asia/Shanghai"))
          )

        ZIO.scoped(
          for {
            emptyVec             <- intCodec.encodeZIO(emptyPayload)
            emptyResult          <- intCodec.decodeZIO(emptyVec)
            stringVec            <- stringCodec.encodeZIO(stringPaylod)
            stringResult         <- stringCodec.decodeZIO(stringVec)
            boolVec              <- boolCodec.encodeZIO(boolPayload)
            boolResult           <- boolCodec.decodeZIO(boolVec)
            byteVec              <- byteCodec.encodeZIO(bytePayload)
            byteResult           <- byteCodec.decodeZIO(byteVec)
            shortVec             <- shortCodec.encodeZIO(shortPayload)
            shortResult          <- shortCodec.decodeZIO(shortVec)
            intVec               <- intCodec.encodeZIO(intPayload)
            intResult            <- intCodec.decodeZIO(intVec)
            longVec              <- longCodec.encodeZIO(longPayload)
            longResult           <- longCodec.decodeZIO(longVec)
            floatVec             <- floatCodec.encodeZIO(floatPayload)
            floatResult          <- floatCodec.decodeZIO(floatVec)
            doubleVec            <- doubleCodec.encodeZIO(doublePayload)
            doubleResult         <- doubleCodec.decodeZIO(doubleVec)
            binaryVec            <- binaryCodec.encodeZIO(binaryPayload)
            binaryResult         <- binaryCodec.decodeZIO(binaryVec)
            charVec              <- charCodec.encodeZIO(charPayload)
            charResult           <- charCodec.decodeZIO(charVec)
            // uuidVec              <- uuidCodec.encodeZIO(uuidPayload)
            // uuidResult           <- uuidCodec.decodeZIO(uuidVec)
            bigDecimalVec        <- bigDecimalCodec.encodeZIO(bigDecimalPayload)
            bigDecimalResult     <- bigDecimalCodec.decodeZIO(bigDecimalVec)
            bigIntegerVec        <- bigIntegerCodec.encodeZIO(bigIntegerPayload)
            bigIntegerResult     <- bigIntegerCodec.decodeZIO(bigIntegerVec)
            dayOfWeekVec         <- dayOfWeekCodec.encodeZIO(dayOfWeekPayload)
            dayOfWeekResult      <- dayOfWeekCodec.decodeZIO(dayOfWeekVec)
            monthVec             <- monthCodec.encodeZIO(monthPayload)
            monthResult          <- monthCodec.decodeZIO(monthVec)
            monthDayVec          <- monthDayCodec.encodeZIO(monthDayPayload)
            monthDayResult       <- monthDayCodec.decodeZIO(monthDayVec)
            // periodVec            <- periodCodec.encodeZIO(periodPayload)
            // periodResult         <- periodCodec.decodeZIO(periodVec)
            yearVec              <- yearCodec.encodeZIO(yearPayload)
            yearResult           <- yearCodec.decodeZIO(yearVec)
            yearMonthVec         <- yearMonthCodec.encodeZIO(yearMonthPayload)
            yearMonthResult      <- yearMonthCodec.decodeZIO(yearMonthVec)
            // zoneIdVec            <- zoneIdCodec.encodeZIO(zoneIdPayload)
            // zoneIdResult         <- zoneIdCodec.decodeZIO(zoneIdVec)
            zoneOffsetVec        <- zoneOffsetCodec.encodeZIO(zoneOffsetPayload)
            zoneOffsetResult     <- zoneOffsetCodec.decodeZIO(zoneOffsetVec)
            durationVec          <- durationCodec.encodeZIO(durationPayload)
            durationResult       <- durationCodec.decodeZIO(durationVec)
            instantVec           <- instantCodec.encodeZIO(instantPayload)
            instantResult        <- instantCodec.decodeZIO(instantVec)
            localDateVec         <- localDateCodec.encodeZIO(localDatePayload)
            localDateResult      <- localDateCodec.decodeZIO(localDateVec)
            localTimeVec         <- localTimeCodec.encodeZIO(localTimePayload)
            localTimeResult      <- localTimeCodec.decodeZIO(localTimeVec)
            localDateTimeVec     <- localDateTimeCodec.encodeZIO(localDateTimePayload)
            localDateTimeResult  <- localDateTimeCodec.decodeZIO(localDateTimeVec)
            offsetTimeVec        <- offsetTimeCodec.encodeZIO(offsetTimePayload)
            offsetTimeResult     <- offsetTimeCodec.decodeZIO(offsetTimeVec)
            offsetDateTimeVec    <- offsetDateTimeCodec.encodeZIO(offsetDateTimePayload)
            offsetDateTimeResult <- offsetDateTimeCodec.decodeZIO(offsetDateTimeVec)
            zonedDateTimeVec     <- zonedDateTimeCodec.encodeZIO(zonedDateTimePayload)
            zonedDateTimeResult  <- zonedDateTimeCodec.decodeZIO(zonedDateTimeVec)
          } yield assertTrue(
            emptyResult == emptyPayload,
            stringResult == stringPaylod,
            boolResult == boolPayload,
            byteResult == bytePayload,
            shortResult == shortPayload,
            intResult == intPayload,
            longResult == longPayload,
            floatResult == floatPayload,
            doubleResult == doublePayload,
            binaryResult == binaryPayload,
            charResult == charPayload,
            // uuidResult == uuidPayload,
            bigDecimalResult == bigDecimalPayload,
            bigIntegerResult == bigIntegerPayload,
            dayOfWeekResult == dayOfWeekPayload,
            monthResult == monthPayload,
            monthDayResult == monthDayPayload,
            // periodResult == periodPayload,
            yearResult == yearPayload,
            yearMonthResult == yearMonthPayload,
            // zoneIdResult == zoneIdPayload,
            zoneOffsetResult == zoneOffsetPayload,
            durationResult == durationPayload,
            instantResult == instantPayload,
            localDateResult == localDatePayload,
            localTimeResult == localTimePayload,
            localDateTimeResult == localDateTimePayload,
            offsetTimeResult == offsetTimePayload,
            offsetDateTimeResult == offsetDateTimePayload,
            zonedDateTimeResult == zonedDateTimePayload
          )
        )
      },
      test("list") {
        import ValueVectorCodec._

        val stringCodec     =
          listChunkCodec(listChunkEncoder[String], listChunkDecoder[String])
        val boolCodec       =
          listChunkCodec(listChunkEncoder[Boolean], listChunkDecoder[Boolean])
        // val byteCodec       =
        //   listChunkCodec(listChunkEncoder[Byte], listChunkDecoder[Byte])
        val shortCodec      =
          listChunkCodec(listChunkEncoder[Short], listChunkDecoder[Short])
        val intCodec        = listChunkCodec(listChunkEncoder[Int], listChunkDecoder[Int])
        val longCodec       = listChunkCodec(listChunkEncoder[Long], listChunkDecoder[Long])
        val floatCodec      = listChunkCodec(listChunkEncoder[Float], listChunkDecoder[Float])
        val doubleCodec     = listChunkCodec(listChunkEncoder[Double], listChunkDecoder[Double])
        // val binaryCodec     = listChunkCodec(listChunkEncoder[Chunk[Byte]], listChunkDecoder[Chunk[Byte]])
        val charCodec       = listChunkCodec(listChunkEncoder[Char], listChunkDecoder[Char])
        // val uuidCodec       = listChunkCodec(listChunkEncoder[java.util.UUID], listChunkDecoder[java.util.UUID])
        val bigDecimalCodec =
          listChunkCodec(listChunkEncoder[java.math.BigDecimal], listChunkDecoder[java.math.BigDecimal])
        val bigIntegerCodec =
          listChunkCodec(listChunkEncoder[java.math.BigInteger], listChunkDecoder[java.math.BigInteger])
        // val dayOfWeekCodec      =
        //   listChunkCodec(listChunkEncoder[java.time.DayOfWeek], listChunkDecoder[java.time.DayOfWeek])
        // val monthCodec          = listChunkCodec(listChunkEncoder[java.time.Month], listChunkDecoder[java.time.Month])
        // val monthDayCodec       = listChunkCodec(listChunkEncoder[java.time.MonthDay], listChunkDecoder[java.time.MonthDay])
        // val periodCodec         = listChunkCodec(listChunkEncoder[java.time.Period], listChunkDecoder[java.time.Period])
        // val yearCodec           = listChunkCodec(listChunkEncoder[java.time.Year], listChunkDecoder[java.time.Year])
        // val yearMonthCodec      =
        //   listChunkCodec(listChunkEncoder[java.time.YearMonth], listChunkDecoder[java.time.YearMonth])
        // val zoneIdCodec         = listChunkCodec(listChunkEncoder[java.time.ZoneId], listChunkDecoder[java.time.ZoneId])
        // val zoneOffsetCodec     =
        //   listChunkCodec(listChunkEncoder[java.time.ZoneOffset], listChunkDecoder[java.time.ZoneOffset])
        // val durationCodec       = listChunkCodec(listChunkEncoder[java.time.Duration], listChunkDecoder[java.time.Duration])
        // val instantCodec        =
        //   listChunkCodec(listChunkEncoder[java.time.Instant], listChunkDecoder[java.time.Instant])
        // val localDateCodec      =
        //   listChunkCodec(listChunkEncoder[java.time.LocalDate], listChunkDecoder[java.time.LocalDate])
        // val localTimeCodec      =
        //   listChunkCodec(listChunkEncoder[java.time.LocalTime], listChunkDecoder[java.time.LocalTime])
        // val localDateTimeCodec  =
        //   listChunkCodec(listChunkEncoder[java.time.LocalDateTime], listChunkDecoder[java.time.LocalDateTime])
        // val offsetTimeCodec     =
        //   listChunkCodec(listChunkEncoder[java.time.OffsetTime], listChunkDecoder[java.time.OffsetTime])
        // val offsetDateTimeCodec =
        //   listChunkCodec(listChunkEncoder[java.time.OffsetDateTime], listChunkDecoder[java.time.OffsetDateTime])
        // val zonedDateTimeCodec  =
        //   listChunkCodec(listChunkEncoder[java.time.ZonedDateTime], listChunkDecoder[java.time.ZonedDateTime])

        val primitivesCodec = listChunkCodec(listChunkEncoder[Primitives], listChunkDecoder[Primitives])

        val stringPayload                     = Chunk(Chunk("zio"), Chunk("cats", "monix"))
        val boolPayload                       = Chunk(Chunk(true), Chunk(false))
        // val bytePayload: Chunk[Chunk[Byte]]          = Chunk(Chunk(100, 99), Chunk(23))
        val shortPayload: Chunk[Chunk[Short]] = Chunk(Chunk(12, 23), Chunk(12))
        val intPayload                        = Chunk(Chunk(-456789, -123456789))
        val longPayload                       = Chunk(Chunk(123L, 90021L))
        val floatPayload                      = Chunk(Chunk(1.2f), Chunk(-3.4f))
        val doublePayload                     = Chunk(Chunk(-1.2d, 3.456789d))
        // val binaryPayload: Chunk[Chunk[Chunk[Byte]]] = Chunk(Chunk(Chunk(1, 2, 3), Chunk(1, 3)))
        val charPayload                       = Chunk(Chunk('a', 'b'))
        // val uuidPayload                              = Chunk(Chunk(java.util.UUID.randomUUID(), java.util.UUID.randomUUID()))
        val bigDecimalPayload                 = Chunk(Chunk(new java.math.BigDecimal("0"), new java.math.BigDecimal("-456789")))
        val bigIntegerPayload                 = Chunk(Chunk(new java.math.BigInteger("123")), Chunk(new java.math.BigInteger("-123")))
        // val dayOfWeekPayload  = Chunk(Chunk(DayOfWeek.MONDAY), Chunk(DayOfWeek.SUNDAY))

        val primitivesPayload = Chunk(Chunk(Primitives(1, 2.0, "3")))

        ZIO.scoped(
          for {
            stringVec        <- stringCodec.encodeZIO(stringPayload)
            stringResult     <- stringCodec.decodeZIO(stringVec)
            boolVec          <- boolCodec.encodeZIO(boolPayload)
            boolResult       <- boolCodec.decodeZIO(boolVec)
            // byteVec          <- byteCodec.encodeZIO(bytePayload)
            // byteResult       <- byteCodec.decodeZIO(byteVec)
            shortVec         <- shortCodec.encodeZIO(shortPayload)
            shortResult      <- shortCodec.decodeZIO(shortVec)
            intVec           <- intCodec.encodeZIO(intPayload)
            intResult        <- intCodec.decodeZIO(intVec)
            longVec          <- longCodec.encodeZIO(longPayload)
            longResult       <- longCodec.decodeZIO(longVec)
            floatVec         <- floatCodec.encodeZIO(floatPayload)
            floatResult      <- floatCodec.decodeZIO(floatVec)
            doubleVec        <- doubleCodec.encodeZIO(doublePayload)
            doubleResult     <- doubleCodec.decodeZIO(doubleVec)
            // binaryVec        <- binaryCodec.encodeZIO(binaryPayload)
            // binaryResult     <- binaryCodec.decodeZIO(binaryVec)
            charVec          <- charCodec.encodeZIO(charPayload)
            charResult       <- charCodec.decodeZIO(charVec)
            // uuidVec          <- uuidCodec.encodeZIO(uuidPayload)
            // uuidResult       <- uuidCodec.decodeZIO(uuidVec)
            bigDecimalVec    <- bigDecimalCodec.encodeZIO(bigDecimalPayload)
            bigDecimalResult <- bigDecimalCodec.decodeZIO(bigDecimalVec)
            bigIntegerVec    <- bigIntegerCodec.encodeZIO(bigIntegerPayload)
            bigIntegerResult <- bigIntegerCodec.decodeZIO(bigIntegerVec)
            primitivesVec    <- primitivesCodec.encodeZIO(primitivesPayload)
            primitivesResult <- primitivesCodec.decodeZIO(primitivesVec)
          } yield assertTrue(
            stringResult == stringPayload,
            boolResult == boolPayload,
            // byteResult == bytePayload,
            shortResult == shortPayload,
            intResult == intPayload,
            longResult == longPayload,
            floatResult == floatPayload,
            doubleResult == doublePayload,
            // binaryResult == binaryPayload,
            charResult == charPayload,
            // uuidResult == uuidPayload,
            bigDecimalResult == bigDecimalPayload,
            bigIntegerResult == bigIntegerPayload,
            primitivesResult == primitivesPayload
          )
        )
      },
      test("struct") {
        import ValueVectorCodec._

        val primitivesCodec = codec(
          Derive.derive[ValueVectorEncoder[StructVector, *], Primitives](
            ValueVectorEncoderDeriver.default[StructVector]
          ),
          Derive.derive[ValueVectorDecoder[StructVector, *], Primitives](
            ValueVectorDecoderDeriver.default[StructVector]
          )
        )

        val primitivesPayload = Chunk(Primitives(1, 2.0, "3"))

        ZIO.scoped(
          for {
            primitivesVec    <- primitivesCodec.encodeZIO(primitivesPayload)
            primitivesResult <- primitivesCodec.decodeZIO(primitivesVec)
          } yield assertTrue(
            primitivesResult == primitivesPayload
          )
        )

        //       test("struct primitives") {
        //         val codec   = ValueVectorCodec.struct[Primitives]
        //         val payload = Chunk(Primitives(1, 2.0, "3"))

        //         ZIO.scoped(
        //           for {
        //             vec    <- codec.encodeZIO(payload)
        //             result <- codec.decodeZIO(vec)
        //           } yield assert(result)(equalTo(payload))
        //         )
        //       },
        //       test("struct of primitives") {
        //         val codec   = ValueVectorCodec.struct[StructOfPrimitives]
        //         val payload = Chunk(StructOfPrimitives(Primitives(1, 2.0, "3")))

        //         ZIO.scoped(
        //           for {
        //             vec    <- codec.encodeZIO(payload)
        //             result <- codec.decodeZIO(vec)
        //           } yield assert(result)(equalTo(payload))
        //         )
        //       },
      },
      test("option") {
        import ValueVectorCodec._

        val stringPayload                      = Chunk(Some("zio"), None, Some("arrow"))
        val shortPayload: Chunk[Option[Short]] = Chunk(Some(3), Some(2), None)
        val intPayload                         = Chunk(Some(1), None, Some(3))

        val optionStringCodec = optionCodec(optionEncoder[VarCharVector, String], optionDecoder[VarCharVector, String])
        val optionShortCodec  = optionCodec(optionEncoder[SmallIntVector, Short], optionDecoder[SmallIntVector, Short])
        val optionIntCodec    = optionCodec(optionEncoder[IntVector, Int], optionDecoder[IntVector, Int])

        ZIO.scoped(
          for {
            stringVec    <- optionStringCodec.encodeZIO(stringPayload)
            stringResult <- optionStringCodec.decodeZIO(stringVec)
            shortVec     <- optionShortCodec.encodeZIO(shortPayload)
            shortResult  <- optionShortCodec.decodeZIO(shortVec)
            intVec       <- optionIntCodec.encodeZIO(intPayload)
            intResult    <- optionIntCodec.decodeZIO(intVec)
          } yield assertTrue(
            stringResult == stringPayload,
            shortResult == shortPayload,
            intResult == intPayload
          )
        )
      }
    )

//   val valueVectorCodecPrimitiveSpec: Spec[BufferAllocator, Throwable] =
//     suite("ValueVectorCodec primitive")(
//       test("empty") {
//         val codec = ValueVectorCodec.primitive[Int, IntVector]

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(Chunk.empty)
//             result <- codec.decodeZIO(vec)
//           } yield assertTrue(result.isEmpty)
//         )
//       },
//       test("string") {
//         val codec   = ValueVectorCodec.primitive[String, VarCharVector]
//         val payload = Chunk("zio", "cats", "monix")

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("boolean") {
//         val codec   = ValueVectorCodec.primitive[Boolean, BitVector]
//         val payload = Chunk(true, true, false)

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("byte") {
//         val codec   = ValueVectorCodec.primitive[Byte, UInt1Vector]
//         val payload = Chunk[Byte](1, 2, 3)

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("short") {
//         val codec   = ValueVectorCodec.primitive[Short, SmallIntVector]
//         val payload = Chunk[Short](1, 2, 3)

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("int") {
//         val codec   = ValueVectorCodec.primitive[Int, IntVector]
//         val payload = Chunk(1, 2, 3)

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("long") {
//         val codec   = ValueVectorCodec.primitive[Long, BigIntVector]
//         val payload = Chunk(1L, 2L, 3L)

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("float") {
//         val codec   = ValueVectorCodec.primitive[Float, Float4Vector]
//         val payload = Chunk(1.0f, 2.0f, 3.0f)

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("double") {
//         val codec   = ValueVectorCodec.primitive[Double, Float8Vector]
//         val payload = Chunk(1.0, 2.0, 3.0)

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("binary") {
//         // TODO: avoid specifying schema explicitly in this case
//         val codec                       =
//           ValueVectorCodec.primitive[Chunk[Byte], LargeVarBinaryVector](zio.schema.Schema.primitive[Chunk[Byte]])
//         val payload: Chunk[Chunk[Byte]] = Chunk(Chunk(1, 2, 3), Chunk(4, 5, 6))

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("char") {
//         val codec   = ValueVectorCodec.primitive[Char, UInt2Vector]
//         val payload = Chunk('a', 'b', 'c')

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("uuid") {
//         val codec   = ValueVectorCodec.primitive[UUID, VarBinaryVector]
//         val payload = Chunk(UUID.randomUUID(), UUID.randomUUID())

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("big decimal") {
//         val codec   = ValueVectorCodec.primitive[java.math.BigDecimal, DecimalVector]
//         val payload = Chunk(new java.math.BigDecimal("12312.33"), new java.math.BigDecimal("9990221.33"))

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("big integer") {
//         val codec   = ValueVectorCodec.primitive[java.math.BigInteger, VarBinaryVector]
//         val payload = Chunk(new java.math.BigInteger("1231233999"), new java.math.BigInteger("9990221001223"))

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("day of week") {
//         val codec   = ValueVectorCodec.primitive[DayOfWeek, IntVector]
//         val payload = Chunk(DayOfWeek.of(1), DayOfWeek.of(2))

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("month") {
//         val codec   = ValueVectorCodec.primitive[Month, IntVector]
//         val payload = Chunk(Month.of(1), Month.of(2))

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("month day") {
//         val codec   = ValueVectorCodec.primitive[MonthDay, BigIntVector]
//         val payload = Chunk(MonthDay.of(1, 31), MonthDay.of(2, 28))

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("period") {
//         val codec   = ValueVectorCodec.primitive[Period, VarBinaryVector]
//         val payload = Chunk(Period.of(1970, 1, 31), Period.of(1990, 2, 28))

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("year") {
//         val codec   = ValueVectorCodec.primitive[Year, IntVector]
//         val payload = Chunk(Year.of(1970), Year.of(1989))

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("year month") {
//         val codec   = ValueVectorCodec.primitive[YearMonth, BigIntVector]
//         val payload = Chunk(YearMonth.of(1970, 1), YearMonth.of(1989, 5))

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("zone id") {
//         val codec   = ValueVectorCodec.primitive[ZoneId, VarCharVector]
//         val payload = Chunk(ZoneId.of("Australia/Sydney"), ZoneId.of("Africa/Harare"))

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("zone offset") {
//         val codec   = ValueVectorCodec.primitive[ZoneOffset, VarCharVector]
//         val payload = Chunk(ZoneOffset.of("+1"), ZoneOffset.of("+3"))

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("duration") {
//         val codec   = ValueVectorCodec.primitive[Duration, BigIntVector]
//         val payload = Chunk(Duration.fromMillis(0), Duration.fromMillis(10092))

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("instant") {
//         val codec   = ValueVectorCodec.primitive[Instant, BigIntVector]
//         val payload = Chunk(Instant.ofEpochMilli(123), Instant.ofEpochMilli(999312))

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("local date") {
//         val codec   = ValueVectorCodec.primitive[LocalDate, VarCharVector]
//         val payload = Chunk(LocalDate.of(1970, 1, 23), LocalDate.of(1989, 5, 31))

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("local time") {
//         val codec   = ValueVectorCodec.primitive[LocalTime, VarCharVector]
//         val payload = Chunk(LocalTime.of(3, 12), LocalTime.of(5, 15))

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("local date time") {
//         val codec   = ValueVectorCodec.primitive[LocalDateTime, VarCharVector]
//         val payload = Chunk(LocalDateTime.of(1970, 1, 3, 3, 12), LocalDateTime.of(1989, 5, 31, 5, 15))

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("offset time") {
//         val codec   = ValueVectorCodec.primitive[OffsetTime, VarCharVector]
//         val payload = Chunk(
//           OffsetTime.of(LocalTime.of(3, 12), ZoneOffset.of("+1")),
//           OffsetTime.of(LocalTime.of(4, 12), ZoneOffset.of("-2"))
//         )

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("offset date time") {
//         val codec   = ValueVectorCodec.primitive[OffsetDateTime, VarCharVector]
//         val payload = Chunk(
//           OffsetDateTime.of(LocalDate.of(1970, 1, 3), LocalTime.of(3, 12), ZoneOffset.of("+1")),
//           OffsetDateTime.of(LocalDate.of(1989, 5, 31), LocalTime.of(4, 12), ZoneOffset.of("-2"))
//         )

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("zoned date time") {
//         val codec   = ValueVectorCodec.primitive[ZonedDateTime, VarCharVector]
//         val payload = Chunk(
//           ZonedDateTime.of(LocalDateTime.of(1970, 1, 3, 3, 12), ZoneId.of("Asia/Dhaka")),
//           ZonedDateTime.of(LocalDateTime.of(1989, 5, 31, 3, 12), ZoneId.of("Asia/Shanghai"))
//         )

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       }
//       //      test("optional string") {
// //        val codec = ValueVectorCodec[Option[String], VarCharVector]
// //        val payload = Chunk(Some("zio"), None, Some("monix"))
// //
// //        ZIO.scoped(
// //          for {
// //            vec <- codec.encodeZIO(payload)
// //            result <- codec.decodeZIO(vec)
// //          } yield assert(result)(equalTo(payload))
// //        )
// //      }
//     )

//   val valueVectorCodecListSpec: Spec[BufferAllocator, Throwable] =
//     suite("ValueVectorCodec list")(
//       test("list empty") {
//         val codec = ValueVectorCodec.list[Int]

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(Chunk.empty)
//             result <- codec.decodeZIO(vec)
//           } yield assertTrue(result.isEmpty)
//         )
//       },
//       test("list string") {
//         val codec   = ValueVectorCodec.list[String]
//         val payload = Chunk(Chunk("zio"), Chunk("cats", "monix"))

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("lit boolean") {
//         val codec   = ValueVectorCodec.list[Boolean]
//         val payload = Chunk(Chunk(true), Chunk(true, false))

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("list byte") {
//         val codec                       = ValueVectorCodec.list[Byte]
//         val payload: Chunk[Chunk[Byte]] = Chunk(Chunk(1), Chunk(2, 3))

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("list short") {
//         val codec                        = ValueVectorCodec.list[Short]
//         val payload: Chunk[Chunk[Short]] = Chunk(Chunk(1), Chunk(2, 3))

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("list int") {
//         val codec   = ValueVectorCodec.list[Int]
//         val payload = Chunk(Chunk(1, 2), Chunk(3))

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("list long") {
//         val codec   = ValueVectorCodec.list[Long]
//         val payload = Chunk(Chunk(1L, 2L), Chunk(3L))

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("list float") {
//         val codec   = ValueVectorCodec.list[Float]
//         val payload = Chunk(Chunk(1.0f, 2.0f), Chunk(3.0f))

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("list double") {
//         val codec   = ValueVectorCodec.list[Double]
//         val payload = Chunk(Chunk(1.0, 2.0), Chunk(3.0))

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("list binary") {
//         val codec                              = ValueVectorCodec.list[Chunk[Byte]]
//         val payload: Chunk[Chunk[Chunk[Byte]]] = Chunk(Chunk(Chunk(1, 2, 3)), Chunk(Chunk(4, 5, 6)))

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("list char") {
//         val codec   = ValueVectorCodec.list[Char]
//         val payload = Chunk(Chunk('a'), Chunk('b', 'c'))

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       // TODO: fix encoder
// //      test("list uuid") {
//       //        val codec   = ValueVectorCodec.list[UUID]
//       //        val payload = Chunk(Chunk(UUID.randomUUID()), Chunk(UUID.randomUUID()))
//       //
//       //        ZIO.scoped(
//       //          for {
//       //            vec    <- codec.encodeZIO(payload)
//       //            result <- codec.decodeZIO(vec)
//       //          } yield assert(result)(equalTo(payload))
//       //        )
//       //      },
//       test("list big decimal") {
//         val codec   = ValueVectorCodec.list[java.math.BigDecimal]
//         val payload = Chunk(Chunk(new java.math.BigDecimal("12312.33")), Chunk(new java.math.BigDecimal("9990221.33")))

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("list big integer") {
//         val codec   = ValueVectorCodec.list[java.math.BigInteger]
//         val payload =
//           Chunk(Chunk(new java.math.BigInteger("1231233999")), Chunk(new java.math.BigInteger("9990221001223")))

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("list day of week") {
//         val codec   = ValueVectorCodec.list[DayOfWeek]
//         val payload = Chunk(Chunk(DayOfWeek.of(1)), Chunk(DayOfWeek.of(2)))

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("list month") {
//         val codec   = ValueVectorCodec.list[Month]
//         val payload = Chunk(Chunk(Month.of(1)), Chunk(Month.of(2)))

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("list month day") {
//         val codec   = ValueVectorCodec.list[MonthDay]
//         val payload = Chunk(Chunk(MonthDay.of(1, 31)), Chunk(MonthDay.of(2, 28)))

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       // TODO: fix encoder
// //      test("list period") {
// //        val codec   = ValueVectorCodec.list[Period]
// //        val payload = Chunk(Chunk(Period.of(1970, 1, 31)), Chunk(Period.of(1990, 2, 28)))
// //
// //        ZIO.scoped(
// //          for {
// //            vec    <- codec.encodeZIO(payload)
// //            result <- codec.decodeZIO(vec)
// //          } yield assert(result)(equalTo(payload))
// //        )
// //      },
//       test("list year") {
//         val codec   = ValueVectorCodec.list[Year]
//         val payload = Chunk(Chunk(Year.of(1970)), Chunk(Year.of(2001)))

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("list year month") {
//         val codec   = ValueVectorCodec.list[YearMonth]
//         val payload = Chunk(Chunk(YearMonth.of(1970, 3)), Chunk(YearMonth.of(2001, 2)))

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("list zone id") {
//         val codec   = ValueVectorCodec.list[ZoneId]
//         val payload = Chunk(Chunk(ZoneId.of("America/St_Johns")), Chunk(ZoneId.of("Europe/Paris")))

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("list zone offset") {
//         val codec   = ValueVectorCodec.list[ZoneOffset]
//         val payload = Chunk(Chunk(ZoneOffset.of("+1")), Chunk(ZoneOffset.of("+2")))

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("list duration") {
//         val codec   = ValueVectorCodec.list[Duration]
//         val payload = Chunk(Chunk(Duration.fromMillis(0)), Chunk(Duration.fromMillis(1000)))

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("list instant") {
//         val codec   = ValueVectorCodec.list[Instant]
//         val payload = Chunk(Chunk(Instant.ofEpochMilli(0)), Chunk(Instant.ofEpochMilli(1000)))

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("list local date") {
//         val codec   = ValueVectorCodec.list[LocalDate]
//         val payload = Chunk(Chunk(LocalDate.of(1970, 1, 1)), Chunk(LocalDate.of(1980, 2, 3)))

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("list local time") {
//         val codec   = ValueVectorCodec.list[LocalTime]
//         val payload = Chunk(Chunk(LocalTime.of(13, 1)), Chunk(LocalTime.of(16, 15)))

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("list local date time") {
//         val codec   = ValueVectorCodec.list[LocalDateTime]
//         val payload = Chunk(Chunk(LocalDateTime.of(1970, 1, 23, 13, 1)), Chunk(LocalDateTime.of(1989, 5, 31, 2, 15)))

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("list offset time") {
//         val codec   = ValueVectorCodec.list[OffsetTime]
//         val payload = Chunk(
//           Chunk(OffsetTime.of(LocalTime.of(13, 1), ZoneOffset.of("+1"))),
//           Chunk(OffsetTime.of(LocalTime.of(16, 15), ZoneOffset.of("+3")))
//         )

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("list offset date time") {
//         val codec   = ValueVectorCodec.list[OffsetDateTime]
//         val payload = Chunk(
//           Chunk(OffsetDateTime.of(LocalDate.of(1970, 1, 1), LocalTime.of(13, 1), ZoneOffset.of("+1"))),
//           Chunk(OffsetDateTime.of(LocalDate.of(1989, 5, 31), LocalTime.of(16, 15), ZoneOffset.of("+3")))
//         )

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("list zoned date time") {
//         val codec   = ValueVectorCodec.list[ZonedDateTime]
//         val payload = Chunk(
//           Chunk(ZonedDateTime.of(LocalDateTime.of(1970, 1, 23, 13, 1), ZoneId.of("Asia/Tokyo"))),
//           Chunk(ZonedDateTime.of(LocalDateTime.of(1989, 5, 31, 8, 30), ZoneId.of("Africa/Cairo")))
//         )

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("list of primitives") {
//         val codec   = ValueVectorCodec.list[List[Int]]
//         val payload = Chunk(Chunk(List(1, 2), List(3)), Chunk(List(4), List(5, 6)))

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("list of structs") {
//         val codec   = ValueVectorCodec.list[Primitives]
//         val payload = Chunk(Chunk(Primitives(1, 2.0, "3"), Primitives(4, 5.0, "6")), Chunk(Primitives(7, 8.0, "9")))

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       }
//     )

//   val valueVectorCodecStructSpec: Spec[BufferAllocator, Throwable] =
//     suite("ValueVectorCodec struct")(
//       test("struct empty") {
//         val codec   = ValueVectorCodec.struct[Primitives]
//         val payload = Chunk.empty[Primitives]

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("struct primitives") {
//         val codec   = ValueVectorCodec.struct[Primitives]
//         val payload = Chunk(Primitives(1, 2.0, "3"))

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("struct of primitives") {
//         val codec   = ValueVectorCodec.struct[StructOfPrimitives]
//         val payload = Chunk(StructOfPrimitives(Primitives(1, 2.0, "3")))

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("struct of lists") {
//         val codec   = ValueVectorCodec.struct[StructOfLists]
//         val payload = Chunk(StructOfLists(ListOfPrimitives(List(1, 2, 3))))

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("struct of structs") {
//         val codec   = ValueVectorCodec.struct[StructOfListsOfStructs]
//         val payload = Chunk(
//           StructOfListsOfStructs(ListOfStructs(List(Primitives(1, 2.0, "3"), Primitives(11, 22.0, "33"))))
//         )

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("struct of lists of structs") {
//         val codec   = ValueVectorCodec.struct[StructOfStructs]
//         val payload = Chunk(StructOfStructs(StructOfPrimitives(Primitives(1, 2.0, "3"))))

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("struct of list of primitives") {
//         val codec   = ValueVectorCodec.struct[ListOfPrimitives]
//         val payload = Chunk(ListOfPrimitives(List(1, 2, 3)))

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("struct of list of structs") {
//         val codec   = ValueVectorCodec.struct[ListOfStructs]
//         val payload = Chunk(ListOfStructs(List(Primitives(1, 2.0, "3"), Primitives(11, 22.0, "33"))))

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("struct of list of lists") {
//         val codec   = ValueVectorCodec.struct[ListOfLists]
//         val payload = Chunk(ListOfLists(List(List(1, 2), List(3))))

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       },
//       test("struct of list of structs of lists") {
//         val codec   = ValueVectorCodec.struct[ListOfStructsOfLists]
//         val payload = Chunk(
//           ListOfStructsOfLists(List(ListOfPrimitives(List(1, 2)), ListOfPrimitives(List(3)))),
//           ListOfStructsOfLists(List(ListOfPrimitives(List(11, 22)), ListOfPrimitives(List(33))))
//         )

//         ZIO.scoped(
//           for {
//             vec    <- codec.encodeZIO(payload)
//             result <- codec.decodeZIO(vec)
//           } yield assert(result)(equalTo(payload))
//         )
//       }
//     )

  // val valueVectorCodecSpec: Spec[BufferAllocator, Throwable] =
  //   suite("ValueVectorCodec")(
  //     // valueVectorDecoderSpec,
  //     // valueVectorEncoderSpec,
  //     valueVectorCodecSpec
  //     // valueVectorCodecListSpec,
  //     // valueVectorCodecStructSpec
  //   )

  // val vectorSchemaRootDecoderSpec: Spec[BufferAllocator, Throwable] =
  //   suite("VectorSchemaRootDecoder")(
  //     test("map") {
  //       val codec = VectorSchemaRootCodec[Primitives]

  //       ZIO.scoped(
  //         for {
  //           root   <- Tabular.empty[Primitives]
  //           _      <- codec.encodeZIO(Chunk(Primitives(1, 2.0, "3")), root)
  //           result <- codec.decoder.map(p => s"${p.a}, ${p.b}, ${p.c}").decodeZIO(root)
  //         } yield assert(result)(equalTo(Chunk("1, 2.0, 3")))
  //       )
  //     }
  //   )

  // val vectorSchemaRootEncoderSpec: Spec[BufferAllocator, Throwable] =
  //   suite("VectorSchemaRootEncoder")(
  //     test("contramap") {
  //       val codec = VectorSchemaRootCodec[Primitives]

  //       ZIO.scoped(
  //         for {
  //           root   <- Tabular.empty[Primitives]
  //           _      <- codec.encoder
  //                       .contramap[String](s => Primitives(s.toInt, s.toDouble, s))
  //                       .encodeZIO(Chunk("1", "2"), root)
  //           result <- codec.decodeZIO(root)
  //         } yield assert(result)(equalTo(Chunk(Primitives(1, 1.0, "1"), Primitives(2, 2.0, "2"))))
  //       )
  //     }
  //   )

  val vectorSchemaRootCodecSpec: Spec[BufferAllocator, Throwable] =
    suite("VectorSchemaRootCodec")(
      test("flat primitives") {
        import VectorSchemaRootCodec._

        val flatPrimitivesCodec = codec(
          Derive.derive[VectorSchemaRootEncoder, Primitives](VectorSchemaRootEncoderDeriver.default),
          Derive.derive[VectorSchemaRootDecoder, Primitives](VectorSchemaRootDecoderDeriver.default)
        )

        val flatPrimitivesPayload = Chunk(Primitives(1, 2.0, "3"), Primitives(4, 5.0, "6"))

        ZIO.scoped(
          for {
            root                 <- Tabular.empty[Primitives]
            flatPrimitivesVec    <- flatPrimitivesCodec.encodeZIO(flatPrimitivesPayload, root)
            flatPrimitivesResult <- flatPrimitivesCodec.decodeZIO(flatPrimitivesVec)
          } yield assert(flatPrimitivesResult)(equalTo(flatPrimitivesPayload))
        )
      }
    )

}
