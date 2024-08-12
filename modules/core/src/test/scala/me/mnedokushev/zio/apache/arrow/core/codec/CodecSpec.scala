package me.mnedokushev.zio.apache.arrow.core.codec

// import me.mnedokushev.zio.apache.arrow.core.Fixtures._
// import me.mnedokushev.zio.apache.arrow.core.{ Allocator, Tabular }
import me.mnedokushev.zio.apache.arrow.core.Fixtures._
import me.mnedokushev.zio.apache.arrow.core.{ Allocator, Tabular }
import org.apache.arrow.memory.BufferAllocator
import zio._
import zio.schema._
import zio.schema.Schema._
import zio.schema.Factory._
import zio.test.Assertion._
import zio.test.{ Spec, _ }
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
        import ValueVectorEncoder._
        import ValueVectorDecoder._
        import ValueVectorCodec._

        val stringCodec         =
          listChunkCodec(listChunkEncoder[String], listChunkDecoder[String])
        val boolCodec           =
          listChunkCodec(listChunkEncoder[Boolean], listChunkDecoder[Boolean])
        val byteCodec           =
          listCodec(listEncoder[Byte, List], listDecoder[Byte, List])
        val shortCodec          =
          listChunkCodec(listChunkEncoder[Short], listChunkDecoder[Short])
        val intCodec            = listChunkCodec(listChunkEncoder[Int], listChunkDecoder[Int])
        val longCodec           = listChunkCodec(listChunkEncoder[Long], listChunkDecoder[Long])
        val floatCodec          = listChunkCodec(listChunkEncoder[Float], listChunkDecoder[Float])
        val doubleCodec         = listChunkCodec(listChunkEncoder[Double], listChunkDecoder[Double])
        // FIX: me.mnedokushev.zio.apache.arrow.core.codec.DecoderError: Failed to cast Primitive(Chunk(1,2,3),binary) to schema Sequence(Primitive(byte,Chunk()), Chunk)
        // val binaryCodec     = listCodec(listEncoder[Chunk[Byte], List], listDecoder[Chunk[Byte], List])
        val charCodec           = listChunkCodec(listChunkEncoder[Char], listChunkDecoder[Char])
        // FIX: Chunk(Chunk(f04b88ba-6004-4e64-a903-f271c4672a95, ec472cee-ab02-0e20-ca14-e3208adfef9e)) was not equal to Chunk(Chunk(644e0460-ba88-4bf0-952a-67c471f203a9, 200e02ab-ee2c-47ec-9eef-df8a20e314ca))
        // val uuidCodec       = listChunkCodec(listChunkEncoder[java.util.UUID], listChunkDecoder[java.util.UUID])
        val bigDecimalCodec     =
          listChunkCodec(listChunkEncoder[java.math.BigDecimal], listChunkDecoder[java.math.BigDecimal])
        val bigIntegerCodec     =
          listChunkCodec(listChunkEncoder[java.math.BigInteger], listChunkDecoder[java.math.BigInteger])
        val dayOfWeekCodec      =
          listChunkCodec(listChunkEncoder[java.time.DayOfWeek], listChunkDecoder[java.time.DayOfWeek])
        val monthCodec          = listChunkCodec(listChunkEncoder[java.time.Month], listChunkDecoder[java.time.Month])
        val monthDayCodec       = listChunkCodec(listChunkEncoder[java.time.MonthDay], listChunkDecoder[java.time.MonthDay])
        // val periodCodec         = listChunkCodec(listChunkEncoder[java.time.Period], listChunkDecoder[java.time.Period])
        val yearCodec           = listChunkCodec(listChunkEncoder[java.time.Year], listChunkDecoder[java.time.Year])
        val yearMonthCodec      =
          listChunkCodec(listChunkEncoder[java.time.YearMonth], listChunkDecoder[java.time.YearMonth])
        val zoneIdCodec         = listChunkCodec(listChunkEncoder[java.time.ZoneId], listChunkDecoder[java.time.ZoneId])
        val zoneOffsetCodec     =
          listChunkCodec(listChunkEncoder[java.time.ZoneOffset], listChunkDecoder[java.time.ZoneOffset])
        val durationCodec       = listChunkCodec(listChunkEncoder[java.time.Duration], listChunkDecoder[java.time.Duration])
        val instantCodec        =
          listChunkCodec(listChunkEncoder[java.time.Instant], listChunkDecoder[java.time.Instant])
        val localDateCodec      =
          listChunkCodec(listChunkEncoder[java.time.LocalDate], listChunkDecoder[java.time.LocalDate])
        val localTimeCodec      =
          listChunkCodec(listChunkEncoder[java.time.LocalTime], listChunkDecoder[java.time.LocalTime])
        val localDateTimeCodec  =
          listChunkCodec(listChunkEncoder[java.time.LocalDateTime], listChunkDecoder[java.time.LocalDateTime])
        val offsetTimeCodec     =
          listChunkCodec(listChunkEncoder[java.time.OffsetTime], listChunkDecoder[java.time.OffsetTime])
        val offsetDateTimeCodec =
          listChunkCodec(listChunkEncoder[java.time.OffsetDateTime], listChunkDecoder[java.time.OffsetDateTime])
        val zonedDateTimeCodec  =
          listChunkCodec(listChunkEncoder[java.time.ZonedDateTime], listChunkDecoder[java.time.ZonedDateTime])

        val primitivesCodec   = listChunkCodec(listChunkEncoder[Primitives], listChunkDecoder[Primitives])
        val optionStringCodec = listChunkOptionCodec(listChunkOptionEncoder[String], listChunkOptionDecoder[String])

        val stringPayload                     = Chunk(Chunk("zio"), Chunk("cats", "monix"))
        val boolPayload                       = Chunk(Chunk(true), Chunk(false))
        val bytePayload: Chunk[List[Byte]]    = Chunk(List(100, 99), List(23))
        val shortPayload: Chunk[Chunk[Short]] = Chunk(Chunk(12, 23), Chunk(12))
        val intPayload                        = Chunk(Chunk(-456789, -123456789))
        val longPayload                       = Chunk(Chunk(123L, 90021L))
        val floatPayload                      = Chunk(Chunk(1.2f), Chunk(-3.4f))
        val doublePayload                     = Chunk(Chunk(-1.2d, 3.456789d))
        // val binaryPayload: Chunk[List[Chunk[Byte]]] = Chunk(List(Chunk(1, 2, 3), Chunk(1, 3)))
        val charPayload                       = Chunk(Chunk('a', 'b'))
        // val uuidPayload                       = Chunk(Chunk(java.util.UUID.randomUUID(), java.util.UUID.randomUUID()))
        val bigDecimalPayload                 = Chunk(Chunk(new java.math.BigDecimal("0"), new java.math.BigDecimal("-456789")))
        val bigIntegerPayload                 = Chunk(Chunk(new java.math.BigInteger("123")), Chunk(new java.math.BigInteger("-123")))
        val dayOfWeekPayload                  = Chunk(Chunk(java.time.DayOfWeek.MONDAY), Chunk(java.time.DayOfWeek.SUNDAY))
        val monthPayload                      = Chunk(Chunk(java.time.Month.JANUARY), Chunk(java.time.Month.DECEMBER))
        val monthDayPayload                   = Chunk(Chunk(java.time.MonthDay.of(1, 2)), Chunk(java.time.MonthDay.of(3, 4)))
        // val periodPayload                     = Chunk(
        //   Chunk(java.time.Period.ofDays(5), java.time.Period.ofWeeks(6)),
        //   Chunk(java.time.Period.ofYears(123))
        // )
        val yearPayload                       = Chunk(Chunk(java.time.Year.of(2024)))
        val yearMonthPayload                  = Chunk(Chunk(java.time.YearMonth.of(4, 3)), Chunk(java.time.YearMonth.of(5, 6)))
        val zoneIdPayload                     = Chunk(
          Chunk(java.time.ZoneId.of("Europe/Paris")),
          Chunk(java.time.ZoneId.of("America/New_York"))
        )
        val zoneOffsetPayload                 = Chunk(
          Chunk(java.time.ZoneOffset.of("+1")),
          Chunk(java.time.ZoneOffset.of("-3"))
        )
        val durationPayload                   = Chunk(
          Chunk(java.time.Duration.ofDays(5)),
          Chunk(java.time.Duration.ofHours(6))
        )
        val instantPayload                    = Chunk(
          Chunk(java.time.Instant.ofEpochMilli(5L))
        )
        val localDatePayload                  = Chunk(
          Chunk(java.time.LocalDate.of(4, 3, 2)),
          Chunk(java.time.LocalDate.of(5, 6, 7))
        )
        val localTimePayload                  = Chunk(
          Chunk(java.time.LocalTime.of(4, 5)),
          Chunk(java.time.LocalTime.of(6, 7))
        )
        val localDateTimePayload              = Chunk(
          Chunk(java.time.LocalDateTime.of(4, 3, 2, 5, 3)),
          Chunk(java.time.LocalDateTime.of(5, 6, 7, 8, 10))
        )
        val offsetTimePayload                 = Chunk(
          Chunk(java.time.OffsetTime.of(java.time.LocalTime.of(13, 1), java.time.ZoneOffset.of("+1"))),
          Chunk(java.time.OffsetTime.of(java.time.LocalTime.of(16, 15), java.time.ZoneOffset.of("+3")))
        )
        val offsetDateTimePayload             = Chunk(
          Chunk(
            java.time.OffsetDateTime
              .of(java.time.LocalDate.of(1970, 1, 1), java.time.LocalTime.of(13, 1), java.time.ZoneOffset.of("+1"))
          ),
          Chunk(
            java.time.OffsetDateTime
              .of(java.time.LocalDate.of(1989, 5, 31), java.time.LocalTime.of(16, 15), java.time.ZoneOffset.of("+3"))
          )
        )
        val zonedDateTimePayload              = Chunk(
          Chunk(
            java.time.ZonedDateTime
              .of(java.time.LocalDateTime.of(2019, 5, 31, 8, 30), java.time.ZoneId.of("Africa/Cairo"))
          ),
          Chunk(
            java.time.ZonedDateTime
              .of(java.time.LocalDateTime.of(2017, 4, 6, 19, 50), java.time.ZoneId.of("Asia/Tokyo"))
          )
        )

        // TODO: think to move them into separate "list of option", "list of struct" test cases
        val primitivesPayload   = Chunk(Chunk(Primitives(1, 2.0, "3")))
        val optionStringPayload = Chunk(Chunk(Some("zio")), Chunk(None, Some("cats")))

        ZIO.scoped(
          for {
            stringVec            <- stringCodec.encodeZIO(stringPayload)
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
            // binaryVec          <- binaryCodec.encodeZIO(binaryPayload)
            // binaryResult       <- binaryCodec.decodeZIO(binaryVec)
            charVec              <- charCodec.encodeZIO(charPayload)
            charResult           <- charCodec.decodeZIO(charVec)
            // uuidVec            <- uuidCodec.encodeZIO(uuidPayload)
            // uuidResult         <- uuidCodec.decodeZIO(uuidVec)
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
            zoneIdVec            <- zoneIdCodec.encodeZIO(zoneIdPayload)
            zoneIdResult         <- zoneIdCodec.decodeZIO(zoneIdVec)
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

            primitivesVec      <- primitivesCodec.encodeZIO(primitivesPayload)
            primitivesResult   <- primitivesCodec.decodeZIO(primitivesVec)
            optionStringVec    <- optionStringCodec.encodeZIO(optionStringPayload)
            optionStringResult <- optionStringCodec.decodeZIO(optionStringVec)
          } yield assertTrue(
            stringResult == stringPayload,
            boolResult == boolPayload,
            byteResult == bytePayload,
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
            dayOfWeekResult == dayOfWeekPayload,
            monthResult == monthPayload,
            monthDayResult == monthDayPayload,
            // periodResult == periodPayload,
            yearResult == yearPayload,
            yearMonthResult == yearMonthPayload,
            zoneIdResult == zoneIdPayload,
            zoneOffsetResult == zoneOffsetPayload,
            durationResult == durationPayload,
            instantResult == instantPayload,
            localDateResult == localDatePayload,
            localTimeResult == localTimePayload,
            localDateTimeResult == localDateTimePayload,
            offsetTimeResult == offsetTimePayload,
            offsetDateTimeResult == offsetDateTimePayload,
            zonedDateTimeResult == zonedDateTimePayload,
            primitivesResult == primitivesPayload,
            optionStringResult == optionStringPayload
          )
        )
      },
      test("struct") {
        import ValueVectorCodec._

        val primitivesCodec             = structCodec[Primitives]
        val structOfPrimitivesCodec     = structCodec[StructOfPrimitives]
        val structOfListsCodec          = structCodec[StructOfLists]
        val structOfListsOfStructsCodec = structCodec[StructOfListsOfStructs]
        val structOfStructsCodec        = structCodec[StructOfStructs]
        val listOfPrimitivesCodec       = structCodec[ListOfPrimitives]
        val listOfStructsCodec          = structCodec[ListOfStructs]
        val listOfListsCodec            = structCodec[ListOfLists]
        val listOfStructsOfListsCodec   = structCodec[ListOfStructsOfLists]

        val primitivesPayload             = Chunk(Primitives(1, 2.0, "3"))
        val structOfPrimitivesPayload     = Chunk(StructOfPrimitives(Primitives(1, 2.0, "4")))
        val structOfListsPayload          = Chunk(StructOfLists(ListOfPrimitives(List(1, 2, 3))))
        val structOfListsOfStructsPayload = Chunk(
          StructOfListsOfStructs(ListOfStructs(List(Primitives(1, 2.0, "3"), Primitives(11, 22.0, "33"))))
        )
        val structOfStructsPayload        = Chunk(StructOfStructs(StructOfPrimitives(Primitives(1, 2.0, "3"))))
        val listOfPrimitivesPayload       = Chunk(ListOfPrimitives(List(1, 2, 3)))
        val listOfStructsPayload          = Chunk(ListOfStructs(List(Primitives(1, 2.0, "3"), Primitives(11, 22.0, "33"))))
        val listOfListsPayload            = Chunk(ListOfLists(List(List(1, 2), List(3))))
        val listOfStructsOfListsPayload   = Chunk(
          ListOfStructsOfLists(List(ListOfPrimitives(List(1, 2)), ListOfPrimitives(List(3)))),
          ListOfStructsOfLists(List(ListOfPrimitives(List(11, 22)), ListOfPrimitives(List(33))))
        )

        ZIO.scoped(
          for {
            primitivesVec                  <- primitivesCodec.encodeZIO(primitivesPayload)
            primitivesResult               <- primitivesCodec.decodeZIO(primitivesVec)
            structOfPrimitivesVec          <- structOfPrimitivesCodec.encodeZIO(structOfPrimitivesPayload)
            structOfPrimitivesResult       <- structOfPrimitivesCodec.decodeZIO(structOfPrimitivesVec)
            structOfListsVec               <- structOfListsCodec.encodeZIO(structOfListsPayload)
            structOfListsResult            <- structOfListsCodec.decodeZIO(structOfListsVec)
            structOfListsOfStructsVec      <- structOfListsOfStructsCodec.encodeZIO(structOfListsOfStructsPayload)
            structOfListsOfStructsResult   <- structOfListsOfStructsCodec.decodeZIO(structOfListsOfStructsVec)
            structOfStructsVec             <- structOfStructsCodec.encodeZIO(structOfStructsPayload)
            structOfStructsResult          <- structOfStructsCodec.decodeZIO(structOfStructsVec)
            structOfListOfPrimitivesVec    <- listOfPrimitivesCodec.encodeZIO(listOfPrimitivesPayload)
            structOfListOfPrimitivesResult <- listOfPrimitivesCodec.decodeZIO(structOfListOfPrimitivesVec)
            listOfStructsVec               <- listOfStructsCodec.encodeZIO(listOfStructsPayload)
            listOfStructsResult            <- listOfStructsCodec.decodeZIO(listOfStructsVec)
            listOfListsVec                 <- listOfListsCodec.encodeZIO(listOfListsPayload)
            listOfListsResult              <- listOfListsCodec.decodeZIO(listOfListsVec)
            listOfStructsOfListVec         <- listOfStructsOfListsCodec.encodeZIO(listOfStructsOfListsPayload)
            listOfListsOfStructsResult     <- listOfStructsOfListsCodec.decodeZIO(listOfStructsOfListVec)
          } yield assertTrue(
            primitivesResult == primitivesPayload,
            structOfPrimitivesResult == structOfPrimitivesPayload,
            structOfListsResult == structOfListsPayload,
            structOfListsOfStructsResult == structOfListsOfStructsPayload,
            structOfStructsResult == structOfStructsPayload,
            structOfListOfPrimitivesResult == listOfPrimitivesPayload,
            listOfStructsResult == listOfStructsPayload,
            listOfListsResult == listOfListsPayload,
            listOfListsOfStructsResult == listOfStructsOfListsPayload
          )
        )
      },
      test("option") {
        import ValueVectorEncoder._
        import ValueVectorDecoder._
        import ValueVectorCodec._

        val stringPayload                      = Chunk(Some("zio"), None, Some("arrow"))
        val shortPayload: Chunk[Option[Short]] = Chunk(Some(3), Some(2), None)
        val intPayload                         = Chunk(Some(1), None, Some(3))
        val listStringPayload                  = Chunk(Some(Chunk("zio", "cats")), None)

        val stringCodec     = optionCodec(optionEncoder[VarCharVector, String], optionDecoder[VarCharVector, String])
        val shortCodec      = optionCodec(optionEncoder[SmallIntVector, Short], optionDecoder[SmallIntVector, Short])
        val intCodec        = optionCodec(optionEncoder[IntVector, Int], optionDecoder[IntVector, Int])
        val listStringCodec = optionListChunkCodec(
          optionListChunkEncoder[String],
          optionListChunkDecoder[String]
        )

        ZIO.scoped(
          for {
            stringVec        <- stringCodec.encodeZIO(stringPayload)
            stringResult     <- stringCodec.decodeZIO(stringVec)
            shortVec         <- shortCodec.encodeZIO(shortPayload)
            shortResult      <- shortCodec.decodeZIO(shortVec)
            intVec           <- intCodec.encodeZIO(intPayload)
            intResult        <- intCodec.decodeZIO(intVec)
            listStringVec    <- listStringCodec.encodeZIO(listStringPayload)
            listStringResult <- listStringCodec.decodeZIO(listStringVec)
          } yield assertTrue(
            stringResult == stringPayload,
            shortResult == shortPayload,
            intResult == intPayload,
            listStringResult == listStringPayload
          )
        )
      }
    )

  val vectorSchemaRootCodecSpec: Spec[BufferAllocator, Throwable] =
    suite("VectorSchemaRootCodec")(
      test("primitives") {
        import VectorSchemaRootEncoder._
        import VectorSchemaRootCodec._

        val primitivesCodec               = codec[Primitives]
        val nullablePrimitivesCodec       = codec[NullablePrimitives]
        val nullableListOfPrimitivesCodec = codec[NullableListOfPrimitives]

        val primitivesPayload               = Chunk(Primitives(1, 2.0, "3"), Primitives(4, 5.0, "6"))
        val nullablePrimitivesPayload       = Chunk(NullablePrimitives(Some(7), None))
        val nullableListOfPrimitivesPayload =
          Chunk(NullableListOfPrimitives(Some(List(1, 2, 3))))

        ZIO.scoped(
          for {
            primitivesRoot                 <- Tabular.empty[Primitives]
            primitivesVec                  <- primitivesCodec.encodeZIO(primitivesPayload, primitivesRoot)
            primitivesResult               <- primitivesCodec.decodeZIO(primitivesVec)
            nullablePrimitivesRoot         <- Tabular.empty[NullablePrimitives]
            nullablePrimitivesVec          <-
              nullablePrimitivesCodec.encodeZIO(nullablePrimitivesPayload, nullablePrimitivesRoot)
            nullablePrimitivesResult       <- nullablePrimitivesCodec.decodeZIO(nullablePrimitivesVec)
            nullableListOfPrimitivesRoot   <- Tabular.empty[NullableListOfPrimitives]
            nullableListOfPrimitivesVec    <-
              nullableListOfPrimitivesCodec.encodeZIO(nullableListOfPrimitivesPayload, nullableListOfPrimitivesRoot)
            nullableListOfPrimitivesResult <-
              nullableListOfPrimitivesCodec.decodeZIO(nullableListOfPrimitivesVec)
          } yield assertTrue(
            primitivesResult == primitivesPayload,
            nullablePrimitivesResult == nullablePrimitivesPayload,
            nullableListOfPrimitivesResult == nullableListOfPrimitivesPayload
          )
        )
      }
    )

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

}
