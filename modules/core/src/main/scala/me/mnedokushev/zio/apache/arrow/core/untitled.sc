import me.mnedokushev.zio.apache.arrow.core.codec.ArrowVectorDecoder
import me.mnedokushev.zio.apache.arrow.core.ArrowAllocator
import me.mnedokushev.zio.apache.arrow.core.vector.ZVector
import org.apache.arrow.memory.RootAllocator
import zio.ZIO
import zio.schema.{ Schema, TypeId }

implicit val alloc = new RootAllocator()

val vec = ZVector.Int.Unsafe(Seq(1, 2, 3))
ArrowVectorDecoder.intDecoder.decode(vec)

val emptyVec = ZVector.Int.Unsafe.empty
ArrowVectorDecoder.intDecoder.decode(emptyVec)

//
//val prog = for {
//  vec <- ZVector.Int(1, 2, 3)
//  dec <- ZIO.fromEither(VectorDecoder.intDecoder.decode(vec))
//} yield dec
//
//prog.provideLayer(ZAllocator.rootLayer())

case class Person(name: String, age: Int, balance: Long)

implicit val schemaPerson: Schema.CaseClass3[String, Int, Long, Person] =
  Schema.CaseClass3[String, Int, Long, Person](
    TypeId.Structural,
    Schema
      .Field[Person, String]("name", Schema.primitive[String], get0 = _.name, set0 = (r, name) => r.copy(name = name)),
    Schema.Field[Person, Int]("age", Schema.primitive[Int], get0 = _.age, set0 = (r, age) => r.copy(age = age)),
    Schema.Field[Person, Long](
      "balance",
      Schema.primitive[Long],
      get0 = _.balance,
      set0 = (r, balance) => r.copy(balance = balance)
    ),
    Person
  )

val emptyStructVec = ZVector.Struct[Person].Unsafe.empty
ArrowVectorDecoder.struct[Person].decode(emptyStructVec)
