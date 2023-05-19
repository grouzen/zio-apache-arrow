package me.mnedokushev.zio.apache.arrow.core

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{ BigIntVector, BitVector, FixedWidthVector, IntVector, ValueVector, VarCharVector }
import zio._

import java.nio.charset.StandardCharsets

trait ZVector[-Val, Vector] {

  def apply(elems: Val*): RIO[BufferAllocator, Vector] =
    wrapUnsafe(unsafe.apply(elems)(_))

  def empty: RIO[BufferAllocator, Vector] =
    wrapUnsafe(unsafe.empty(_))

  def fromChunk(chunk: Chunk[Val]): RIO[BufferAllocator, Vector] =
    wrapUnsafe(unsafe.fromChunk(chunk)(_))

  def fromIterable(it: Iterable[Val]): RIO[BufferAllocator, Vector] =
    wrapUnsafe(unsafe.fromIterable(it)(_))

  trait Unsafe {

    def apply(elems: Seq[Val])(implicit alloc: BufferAllocator): Vector =
      fromIterable(elems.iterator.to(Iterable))

    def empty(implicit alloc: BufferAllocator): Vector

    def fromChunk(chunk: Chunk[Val])(implicit alloc: BufferAllocator): Vector =
      fromIterable(chunk.iterator.to(Iterable))

    def fromIterable(it: Iterable[Val])(implicit alloc: BufferAllocator): Vector

  }

  val unsafe: Unsafe

  private def wrapUnsafe(unsafe: BufferAllocator => Vector): RIO[BufferAllocator, Vector] =
    ZIO.serviceWithZIO[BufferAllocator] { alloc =>
      ZIO.attempt(unsafe(alloc))
    }

}

object ZVector {

  final object Boolean
      extends ZVectorPrimitive[Boolean, BitVector](
        new BitVector("bitVector", _)
      )(_.allocateNew)(vec => (i, v) => vec.set(i, if (v) 1 else 0))

  final object Int extends ZVectorPrimitive[Int, IntVector](new IntVector("intVector", _))(_.allocateNew)(_.set)

  final object Long
      extends ZVectorPrimitive[Long, BigIntVector](new BigIntVector("longVector", _))(_.allocateNew)(_.set)

  final object String
      extends ZVectorPrimitive[String, VarCharVector](
        new VarCharVector("stringVector", _)
      )(_.allocateNew)(vec => (i, v) => vec.set(i, v.getBytes(StandardCharsets.UTF_8)))

  abstract class ZVectorPrimitive[-Val, Vector <: ValueVector](makeVec: BufferAllocator => Vector)(
    allocNew: Vector => Int => Unit
  )(
    setVal: Vector => (Int, Val) => Unit
  ) extends ZVector[Val, Vector] {

    override val unsafe: Unsafe = new Unsafe {

      override def empty(implicit alloc: BufferAllocator): Vector = {
        val vec = makeVec(alloc)

        vec.setValueCount(0)
        vec
      }

      override def fromIterable(it: Iterable[Val])(implicit alloc: BufferAllocator): Vector = {
        val vec = makeVec(alloc)
        val len = it.size

        allocNew(vec)(len)
        it.zipWithIndex.foreach { case (v, i) => setVal(vec)(i, v) }
        vec.setValueCount(len)
        vec
      }
    }

  }

}
