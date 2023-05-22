package me.mnedokushev.zio.apache.arrow.core.vector

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import zio._
import zio.schema._

import java.nio.charset.StandardCharsets

trait ZVector[Val, Vector <: AutoCloseable] {

  protected def fromUnsafe(unsafe: BufferAllocator => Vector): RIO[Scope with BufferAllocator, Vector] =
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

  final object ListBoolean extends ZVectorList[Boolean](writer => v => writer.writeBit(if (v) 1 else 0))

  final object ListInt extends ZVectorList[Int](_.writeInt)

  final object ListLong extends ZVectorList[Long](_.writeBigInt)

  final def Struct[A: Schema]: ZVectorStruct[A] = new ZVectorStruct[A] {}

}
