package me.mnedokushev.zio.apache.arrow.core

import org.apache.arrow.memory.RootAllocator
import zio._

object ArrowAllocator {

  def root(limit: Long = Long.MaxValue): ZIO[Scope, Throwable, RootAllocator] =
    ZIO.acquireRelease(
      ZIO.attempt(new RootAllocator(limit))
    )(a => ZIO.attempt(a.close()).ignoreLogged)

  def rootLayer(limit: Long = Long.MaxValue): TaskLayer[RootAllocator] =
    ZLayer.scoped(root(limit))

}
