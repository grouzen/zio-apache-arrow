import me.mnedokushev.zio.apache.arrow.core.codec.VectorDecoder
import me.mnedokushev.zio.apache.arrow.core.ZAllocator
import me.mnedokushev.zio.apache.arrow.core.vector.ZVector
import org.apache.arrow.memory.RootAllocator
import zio.ZIO

implicit val alloc = new RootAllocator()

val vec = ZVector.Int.unsafe.apply(Seq(1, 2, 3))

val vecZio = ZVector.Int(1, 2, 3)

VectorDecoder.intDecoder.decode(vec)

val emptyVec = ZVector.Int.unsafe.empty

VectorDecoder.intDecoder.decode(emptyVec)

val prog = for {
  vec <- ZVector.Int(1, 2, 3)
  dec <- ZIO.fromEither(VectorDecoder.intDecoder.decode(vec))
} yield dec

prog.provideLayer(ZAllocator.rootLayer())
