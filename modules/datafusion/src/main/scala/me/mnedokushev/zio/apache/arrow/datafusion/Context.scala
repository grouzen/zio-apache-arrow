package me.mnedokushev.zio.apache.arrow.datafusion

import org.apache.arrow.datafusion._
import zio._

import java.nio.file.Path

class Context(underlying: SessionContext) {

  def sql(query: String): Task[Dataframe] =
    ZIO.fromCompletableFuture(underlying.sql(query)).map(new Dataframe(_))

  def registerCsv(name: String, path: Path): Task[Unit] =
    ZIO.fromCompletableFuture(underlying.registerCsv(name, path)).unit

  def registerParquet(name: String, path: Path): Task[Unit] =
    ZIO.fromCompletableFuture(underlying.registerParquet(name, path)).unit

}

object Context {

  def create: TaskLayer[Context] =
    ZLayer.scoped(
      ZIO
        .fromAutoCloseable(ZIO.attempt(SessionContexts.create()))
        .map(new Context(_))
    )

}
