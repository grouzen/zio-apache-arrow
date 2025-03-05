//> using scala "3.4.3"
//> using dep me.mnedokushev::zio-apache-arrow-core:0.1.7
//> using dep org.apache.arrow:arrow-memory-unsafe:18.2.0
//> using javaOpt --add-opens=java.base/java.nio=ALL-UNNAMED

import me.mnedokushev.zio.apache.arrow.core.Allocator
import me.mnedokushev.zio.apache.arrow.core.codec.*
import zio.*
import zio.schema.*
import zio.schema.Factory.*

object ValueVectorCodecs extends ZIOAppDefault:

  case class MyData(a: Int, b: String)

  object MyData:
    implicit val schema: Schema[MyData] = DeriveSchema.gen[MyData]

  val intCodec          = ValueVectorCodec.intCodec
  val listStringCodec   = ValueVectorCodec.listChunkCodec[String]
  val structMyDataCodec = ValueVectorCodec.structCodec[MyData]

  override def run =
    ZIO
      .scoped(
        for {
          intVec             <- intCodec.encodeZIO(Chunk(1, 2, 3))
          intResult          <- intCodec.decodeZIO(intVec)
          listStringVec      <- listStringCodec.encodeZIO(Chunk(Chunk("a", "b"), Chunk("c")))
          listStringResult   <- listStringCodec.decodeZIO(listStringVec)
          structMyDataVec    <- structMyDataCodec.encodeZIO(Chunk(MyData(1, "a"), MyData(2, "b")))
          structMyDataResult <- structMyDataCodec.decodeZIO(structMyDataVec)
          _                  <- Console.printLine(intVec)
          _                  <- Console.printLine(intResult)
          _                  <- Console.printLine(listStringVec)
          _                  <- Console.printLine(listStringResult)
          _                  <- Console.printLine(structMyDataVec)
          _                  <- Console.printLine(structMyDataResult)
        } yield ()
      )
      .provide(Allocator.rootLayer())
  // Outputs:
  // [1, 2, 3]
  // Chunk(1,2,3)
  // [["a","b"], ["c"]]
  // Chunk(Chunk(a,b),Chunk(c))
  // [{"a":1,"b":"a"}, {"a":2,"b":"b"}]
  // Chunk(MyData(1,a),MyData(2,b))
