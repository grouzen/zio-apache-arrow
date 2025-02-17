//> using scala "3.4.3"
//> using dep me.mnedokushev::zio-apache-arrow-core:0.1.6
//> using dep org.apache.arrow:arrow-memory-unsafe:18.2.0
//> using javaOpt --add-opens=java.base/java.nio=ALL-UNNAMED

import me.mnedokushev.zio.apache.arrow.core.Allocator
import me.mnedokushev.zio.apache.arrow.core.codec.*
import me.mnedokushev.zio.apache.arrow.core.ipc.*
import java.io.ByteArrayInputStream
import zio.*
import zio.stream.*
import zio.schema.*
import zio.schema.Factory.*

object IPC extends ZIOAppDefault:

  case class MyData(a: Int, b: String)

  object MyData:
    implicit val schema: Schema[MyData]                   =
      DeriveSchema.gen[MyData]
    implicit val schemaEncoder: SchemaEncoder[MyData]     =
      SchemaEncoder.fromDefaultDeriver[MyData]
    implicit val encoder: VectorSchemaRootDecoder[MyData] =
      VectorSchemaRootDecoder.fromDefaultDeriver[MyData]
    implicit val decoder: VectorSchemaRootEncoder[MyData] =
      VectorSchemaRootEncoder.fromDefaultDeriver[MyData]

  val payload = (1 to 8096).map(i => MyData(i, s"string ${i.toString}"))

  override def run =
    ZIO
      .scoped(
        for {
          out    <- writeStreaming[Any, MyData](ZStream.from(payload))
          result <- readStreaming[MyData](new ByteArrayInputStream(out.toByteArray)).runCollect
          _      <- Console.printLine(result)
        } yield ()
      )
      .provide(Allocator.rootLayer())
  // Outputs:
  // Chunk(MyData(1,string 1), ..., MyData(8096,string 8096))
