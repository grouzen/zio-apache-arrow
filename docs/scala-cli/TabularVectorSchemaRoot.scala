//> using scala "3.4.3"
//> using dep me.mnedokushev::zio-apache-arrow-core:0.1.5
//> using dep org.apache.arrow:arrow-memory-unsafe:18.2.0
//> using javaOpt --add-opens=java.base/java.nio=ALL-UNNAMED

import me.mnedokushev.zio.apache.arrow.core.Allocator
import me.mnedokushev.zio.apache.arrow.core.codec.*
import me.mnedokushev.zio.apache.arrow.core.Tabular.*
import me.mnedokushev.zio.apache.arrow.core.Tabular
import zio.*
import zio.schema.*
import zio.schema.Factory.*

object TabularVectorSchemaRoot extends ZIOAppDefault:

  case class MyData(a: Int, b: String)

  object MyData:
    implicit val schema: Schema[MyData]               =
      DeriveSchema.gen[MyData]
    implicit val schemaEncoder: SchemaEncoder[MyData] =
      SchemaEncoder.fromDefaultDeriver[MyData]

  val myDataCodec = VectorSchemaRootCodec.codec[MyData]

  override def run =
    ZIO
      .scoped(
        for {
          root         <- Tabular.empty[MyData]
          myDataVec    <- myDataCodec.encodeZIO(Chunk(MyData(1, "a"), MyData(2, "b")), root)
          myDataResult <- myDataCodec.decodeZIO(myDataVec)
          _            <- Console.printLine(myDataResult)
        } yield ()
      )
      .provide(Allocator.rootLayer())
  // Outputs:
  // Chunk(MyData(1,a),MyData(2,b))
