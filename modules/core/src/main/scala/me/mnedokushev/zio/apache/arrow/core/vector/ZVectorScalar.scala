package me.mnedokushev.zio.apache.arrow.core.vector

import me.mnedokushev.zio.apache.arrow.core.codec.VectorDecoder
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ValueVector
import zio._

abstract class ZVectorScalar[Val, Vector <: ValueVector](makeVec: BufferAllocator => Vector)(
  allocNew: Vector => Int => Unit
)(
  setVal: Vector => (Int, Val) => Unit
) extends ZVector[Val, Vector] {

  def apply(elems: Val*): RIO[Scope with BufferAllocator, Vector] =
    fromUnsafe(Unsafe.apply(elems)(_))

  def decodeZIO(vec: Vector)(implicit decoder: VectorDecoder[Vector, Val]): Task[Chunk[Val]] =
    decoder.decodeZIO(vec)

  def empty: RIO[Scope with BufferAllocator, Vector] =
    fromUnsafe(Unsafe.empty(_))

  def fromChunk(chunk: Chunk[Val]): RIO[Scope with BufferAllocator, Vector] =
    fromUnsafe(Unsafe.fromChunk(chunk)(_))

  def fromIterable(it: Iterable[Val]): RIO[Scope with BufferAllocator, Vector] =
    fromUnsafe(Unsafe.fromIterable(it)(_))

  object Unsafe {

    def apply(elems: Seq[Val])(implicit alloc: BufferAllocator): Vector =
      this.fromIterable(elems.to(Iterable))

    def empty(implicit alloc: BufferAllocator): Vector = {
      val vec = makeVec(alloc)

      vec.setValueCount(0)
      vec
    }

    def fromChunk(chunk: Chunk[Val])(implicit alloc: BufferAllocator): Vector =
      this.fromIterable(chunk.to(Iterable))

    def fromIterable(it: Iterable[Val])(implicit alloc: BufferAllocator): Vector = {
      val vec = makeVec(alloc)
      val len = it.size

      allocNew(vec)(len)
      it.zipWithIndex.foreach { case (v, i) => setVal(vec)(i, v) }
      vec.setValueCount(len)
      vec
    }

  }

}
