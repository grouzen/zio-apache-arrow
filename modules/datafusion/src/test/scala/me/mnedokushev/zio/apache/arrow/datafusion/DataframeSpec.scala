package me.mnedokushev.zio.apache.arrow.datafusion

import me.mnedokushev.zio.apache.arrow.core.Allocator
import me.mnedokushev.zio.apache.arrow.core.codec.{ SchemaEncoder, SchemaEncoderDeriver }
import zio._
import zio.schema._
import zio.test.Assertion._
import zio.test._

import java.nio.file.Paths
import me.mnedokushev.zio.apache.arrow.core.codec.VectorSchemaRootDecoder

object DataframeSpec extends ZIOSpecDefault {

  case class TestData(fname: String, lname: String, address: String, age: Long)
  object TestData {
    implicit val schema: Schema[TestData]                                   =
      DeriveSchema.gen[TestData]
    implicit val schemaEncoder: SchemaEncoder[TestData]                     =
      Derive.derive[SchemaEncoder, TestData](SchemaEncoderDeriver.default)
    implicit val vectorSchemaRootDecoder: VectorSchemaRootDecoder[TestData] =
      VectorSchemaRootDecoder.decoder[TestData]

  }

  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("Dataframe")(
      test("collect") {
        ZIO.serviceWithZIO[Context] { context =>
          for {
            _      <- context.registerCsv("test", Paths.get(getClass.getResource("/test.csv").toURI))
            df     <- context.sql("SELECT * FROM test WHERE fname = 'Dog'")
            result <- df.collect[TestData].runCollect
          } yield assert(result)(equalTo(Chunk(TestData("Dog", "Cat", "NY", 3))))
        }
      }
    ).provide(Context.create, Allocator.rootLayer())

}
