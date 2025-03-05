//> using scala "3.4.3"
//> using dep me.mnedokushev::zio-apache-arrow-core:0.1.7
//> using dep org.apache.arrow:arrow-memory-unsafe:18.2.0
//> using javaOpt --add-opens=java.base/java.nio=ALL-UNNAMED

import me.mnedokushev.zio.apache.arrow.core.Allocator
import me.mnedokushev.zio.apache.arrow.core.codec.*
import org.apache.arrow.vector.IntVector
import zio.*
import zio.schema.*
import zio.schema.Factory.*

object ValueVectorCodecsNullable extends ZIOAppDefault:

  val intCodec              = ValueVectorCodec.optionCodec[IntVector, Int]
  val listStringCodec       = ValueVectorCodec.listChunkOptionCodec[String]
  val optionListStringCodec = ValueVectorCodec.optionListChunkCodec[String]

  override def run =
    ZIO
      .scoped(
        for {
          intVec                 <- intCodec.encodeZIO(Chunk(Some(1), None, Some(2)))
          intResult              <- intCodec.decodeZIO(intVec)
          listStringVec          <- listStringCodec.encodeZIO(Chunk(Chunk(Some("a"), None), Chunk(Some("b"))))
          listStringResult       <- listStringCodec.decodeZIO(listStringVec)
          optionListStringVec    <- optionListStringCodec.encodeZIO(Chunk(Some(Chunk("a", "b")), None, Some(Chunk("c"))))
          optionListStringResult <- optionListStringCodec.decodeZIO(optionListStringVec)
          _                      <- Console.printLine(intVec)
          _                      <- Console.printLine(intResult)
          _                      <- Console.printLine(listStringVec)
          _                      <- Console.printLine(listStringResult)
          _                      <- Console.printLine(optionListStringVec)
          _                      <- Console.printLine(optionListStringResult)
        } yield ()
      )
      .provide(Allocator.rootLayer())
  // Outputs:
  // [1, null, 2]
  // Chunk(Some(1),None,Some(2))
  // [["a",null], ["b"]]
  // Chunk(Chunk(Some(a),None),Chunk(None))
  // [["a","b"], null, ["c"]]
  // Chunk(Some(Chunk(a,b)),None,Some(Chunk(c)))
