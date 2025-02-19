//> using scala "3.4.3"
//> using dep me.mnedokushev::zio-apache-arrow-core:0.1.6
//> using dep org.apache.arrow:arrow-memory-unsafe:18.2.0
//> using javaOpt --add-opens=java.base/java.nio=ALL-UNNAMED

import me.mnedokushev.zio.apache.arrow.core.codec.*
import zio.*
import zio.schema.*

object TabularSchemaEncoder extends ZIOAppDefault:

  case class MyData(a: Int, b: String)

  object MyData:
    implicit val schema: Schema[MyData]               =
      DeriveSchema.gen[MyData]
    implicit val schemaEncoder: SchemaEncoder[MyData] =
      SchemaEncoder.fromDefaultDeriver[MyData]

  override def run =
    for {
      schema <- ZIO.fromEither(MyData.schemaEncoder.encode(MyData.schema))
      _      <- Console.printLine(schema)
    } yield ()
  // Outputs:
  // Schema<a: Int(64, true) not null, b: Utf8 not null>
