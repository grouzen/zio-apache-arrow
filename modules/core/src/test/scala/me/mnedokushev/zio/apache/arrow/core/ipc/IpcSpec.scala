package me.mnedokushev.zio.apache.arrow.core.ipc

import me.mnedokushev.zio.apache.arrow.core.Allocator
import me.mnedokushev.zio.apache.arrow.core.Fixtures.Primitives
import zio._
import zio.stream.ZStream
import zio.test.Assertion._
import zio.test._

import java.io.ByteArrayInputStream

object IpcSpec extends ZIOSpecDefault {
  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("IPC")(
      test("streaming") {
        val payload = (1 to 8096).map(i => Primitives(i, i.toDouble, i.toString))

        ZIO.scoped(
          for {
            out    <- writeStreaming[Any, Primitives](ZStream.from(payload))
            result <- readStreaming[Primitives](new ByteArrayInputStream(out.toByteArray)).runCollect
          } yield assert(result)(equalTo(Chunk.fromIterable(payload)))
        )
      }
    ) provideLayerShared (Allocator.rootLayer())

}
