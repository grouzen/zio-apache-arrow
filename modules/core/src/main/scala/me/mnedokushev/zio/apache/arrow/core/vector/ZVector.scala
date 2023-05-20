package me.mnedokushev.zio.apache.arrow.core.vector

import me.mnedokushev.zio.apache.arrow.core.codec.VectorDecoder
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.complex.impl.UnionListWriter
import zio._

import java.nio.charset.StandardCharsets

trait ZVector[Val, Vector <: AutoCloseable] {

  protected def unsafe(unsafe: BufferAllocator => Vector): RIO[Scope with BufferAllocator, Vector] =
    ZIO.fromAutoCloseable(
      ZIO.serviceWithZIO[BufferAllocator] { alloc =>
        ZIO.attempt(unsafe(alloc))
      }
    )

}

object ZVector {

  final object Boolean
      extends ZVectorScalar[Boolean, BitVector](
        new BitVector("bitVector", _)
      )(_.allocateNew)(vec => (i, v) => vec.set(i, if (v) 1 else 0))

  final object Int extends ZVectorScalar[Int, IntVector](new IntVector("intVector", _))(_.allocateNew)(_.set)

  final object Long extends ZVectorScalar[Long, BigIntVector](new BigIntVector("longVector", _))(_.allocateNew)(_.set)

  final object String
      extends ZVectorScalar[String, VarCharVector](
        new VarCharVector("stringVector", _)
      )(_.allocateNew)(vec => (i, v) => vec.set(i, v.getBytes(StandardCharsets.UTF_8)))

  final object ListBoolean extends ZVectorList[Boolean](vec => v => vec.writeBit(if (v) 1 else 0))

  final object ListInt extends ZVectorList[Int](_.writeInt)

  final object ListLong extends ZVectorList[Long](_.writeBigInt)

  abstract class ZVectorScalar[Val, Vector <: ValueVector](makeVec: BufferAllocator => Vector)(
    allocNew: Vector => Int => Unit
  )(
    setVal: Vector => (Int, Val) => Unit
  ) extends ZVector[Val, Vector] {

    def apply(elems: Val*): RIO[Scope with BufferAllocator, Vector] =
      unsafe(Unsafe.apply(elems)(_))

    def decodeZIO(vec: Vector)(implicit decoder: VectorDecoder[Vector, Val]): Task[Chunk[Val]] =
      decoder.decodeZIO(vec)

    def empty: RIO[Scope with BufferAllocator, Vector] =
      unsafe(Unsafe.empty(_))

    def fromChunk(chunk: Chunk[Val]): RIO[Scope with BufferAllocator, Vector] =
      unsafe(Unsafe.fromChunk(chunk)(_))

    def fromIterable(it: Iterable[Val]): RIO[Scope with BufferAllocator, Vector] =
      unsafe(Unsafe.fromIterable(it)(_))

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

  abstract class ZVectorList[Val](writeVal: UnionListWriter => Val => Unit) extends ZVector[Val, ListVector] {

    def decodeZIO(vec: ListVector)(implicit decoder: VectorDecoder[ListVector, List[Val]]): Task[Chunk[List[Val]]] =
      decoder.decodeZIO(vec)

    def apply(elems: List[Val]*): RIO[Scope with BufferAllocator, ListVector] =
      unsafe(Unsafe.apply(elems)(_))

    def empty: RIO[Scope with BufferAllocator, ListVector] =
      unsafe(Unsafe.empty(_))

    def fromChunk[Col[x] <: Iterable[x]](chunk: Chunk[Col[Val]]): RIO[Scope with BufferAllocator, ListVector] =
      unsafe(Unsafe.fromChunk(chunk)(_))

    def fromIterable(it: Iterable[Iterable[Val]]): RIO[Scope with BufferAllocator, ListVector] =
      unsafe(Unsafe.fromIterable(it)(_))

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

}
