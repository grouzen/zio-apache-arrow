package me.mnedokushev.zio.apache.arrow.core.vector

import me.mnedokushev.zio.apache.arrow.core.codec.VectorDecoder
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.complex.impl.UnionListWriter
import zio._

abstract class ZVectorList[Val](writeVal: UnionListWriter => Val => Unit) extends ZVector[Val, ListVector] {

  def decodeZIO(vec: ListVector)(implicit decoder: VectorDecoder[ListVector, List[Val]]): Task[Chunk[List[Val]]] =
    decoder.decodeZIO(vec)

  def apply(elems: List[Val]*): RIO[Scope with BufferAllocator, ListVector] =
    fromUnsafe(Unsafe.apply(elems)(_))

  def empty: RIO[Scope with BufferAllocator, ListVector] =
    fromUnsafe(Unsafe.empty(_))

  def fromChunk[Col[x] <: Iterable[x]](chunk: Chunk[Col[Val]]): RIO[Scope with BufferAllocator, ListVector] =
    fromUnsafe(Unsafe.fromChunk(chunk)(_))

  def fromIterable(it: Iterable[Iterable[Val]]): RIO[Scope with BufferAllocator, ListVector] =
    fromUnsafe(Unsafe.fromIterable(it)(_))

  object Unsafe {

    def apply(elems: Seq[List[Val]])(implicit alloc: BufferAllocator): ListVector =
      fromIterable(elems.to(Iterable))

    def empty(implicit alloc: BufferAllocator): ListVector =
      ListVector.empty("listVector", alloc)

    def fromChunk[Col[x] <: Iterable[x]](chunk: Chunk[Col[Val]])(implicit alloc: BufferAllocator): ListVector =
      this.fromIterable(chunk.to(Iterable))

    def fromIterable(it: Iterable[Iterable[Val]])(implicit alloc: BufferAllocator): ListVector = {
      val vec    = this.empty
      val len    = it.size
      val writer = vec.getWriter

      it.zipWithIndex.foreach { case (v, i) =>
        val it0  = v.to(Iterable)
        val len0 = it0.size

        writer.startList()
        writer.setPosition(i)
        it0.foreach(writeVal(writer)(_))
        writer.setValueCount(len0)
        writer.endList()
      }
      vec.setValueCount(len)
      vec
    }

  }

}
