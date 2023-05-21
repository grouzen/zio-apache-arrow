package me.mnedokushev.zio.apache.arrow.core.vector

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import zio._
import zio.schema.Schema

trait ZRecordBatch[Val] {

  def empty(implicit schema: Schema[Val]): RIO[Scope with BufferAllocator, VectorSchemaRoot]

}

object ZRecordBatch {}
