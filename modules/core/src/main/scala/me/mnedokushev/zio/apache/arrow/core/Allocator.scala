package me.mnedokushev.zio.apache.arrow.core

import org.apache.arrow.memory.RootAllocator
import zio._

object Allocator {

  def root(limit: Long = Long.MaxValue): ZIO[Scope, Throwable, RootAllocator] =
    ZIO.fromAutoCloseable(ZIO.attempt(new RootAllocator(limit)))

  def rootLayer(limit: Long = Long.MaxValue): TaskLayer[RootAllocator] =
    ZLayer.scoped(root(limit))

}
