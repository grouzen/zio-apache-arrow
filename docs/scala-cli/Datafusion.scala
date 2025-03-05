//> using scala "3.4.3"
//> using dep me.mnedokushev::zio-apache-arrow-core:0.1.7
//> using dep me.mnedokushev::zio-apache-arrow-datafusion:0.1.7
//> using dep org.apache.arrow:arrow-memory-unsafe:18.2.0
//> using javaOpt --add-opens=java.base/java.nio=ALL-UNNAMED

import me.mnedokushev.zio.apache.arrow.core.Allocator
import me.mnedokushev.zio.apache.arrow.core.codec.*
import me.mnedokushev.zio.apache.arrow.datafusion.*
import zio.*
import zio.schema.*

import java.nio.file.Paths
import java.io.File

object Datafusion extends ZIOAppDefault:

  case class User(fname: String, lname: String, address: String, age: Long)

  object User:
    implicit val schema: Schema[User]                                   =
      DeriveSchema.gen[User]
    implicit val schemaEncoder: SchemaEncoder[User]                     =
      Derive.derive[SchemaEncoder, User](SchemaEncoderDeriver.default)
    implicit val vectorSchemaRootDecoder: VectorSchemaRootDecoder[User] =
      VectorSchemaRootDecoder.fromDefaultDeriver[User]

  override def run =
    (
      ZIO
        .serviceWithZIO[Context] { context =>
          for {
            _      <- context.registerCsv("test", Paths.get(new File("test.csv").toURI))
            df     <- context.sql("SELECT * FROM test WHERE fname = 'Dog'")
            result <- df.collect[User].runCollect
            _      <- Console.printLine(result)
          } yield ()
        }
      )
      .provide(Context.create, Allocator.rootLayer())
  // Outputs:
  // Chunk(User(Dog,Cat,NY,3))
