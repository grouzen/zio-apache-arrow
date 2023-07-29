package me.mnedokushev.zio.apache.arrow.core.codec

import zio.schema._

object Fixtures {

  final case class Primitives(a: Int, b: Double, c: String)
  object Primitives {
    implicit val schema: Schema[Primitives] = DeriveSchema.gen[Primitives]
  }

  final case class StructOfPrimitives(struct: Primitives)
  object StructOfPrimitives {
    implicit val schema: Schema[StructOfPrimitives] = DeriveSchema.gen[StructOfPrimitives]
  }

  final case class StructOfLists(struct: ListOfPrimitives)
  object StructOfLists {
    implicit val schema: Schema[StructOfLists] = DeriveSchema.gen[StructOfLists]
  }

  final case class StructOfStructs(struct: StructOfPrimitives)
  object StructOfStructs {
    implicit val schema: Schema[StructOfStructs] = DeriveSchema.gen[StructOfStructs]
  }

  final case class StructOfListsOfStructs(struct: ListOfStructs)
  object StructOfListsOfStructs {
    implicit val schema: Schema[StructOfListsOfStructs] = DeriveSchema.gen[StructOfListsOfStructs]
  }

  final case class ListOfPrimitives(list: List[Int])
  object ListOfPrimitives {
    implicit val schema: Schema[ListOfPrimitives] = DeriveSchema.gen[ListOfPrimitives]
  }

  final case class ListOfStructs(list: List[Primitives])
  object ListOfStructs {
    implicit val schema: Schema[ListOfStructs] = DeriveSchema.gen[ListOfStructs]
  }

  final case class ListOfLists(list: List[List[Int]])
  object ListOfLists {
    implicit val schema: Schema[ListOfLists] = DeriveSchema.gen[ListOfLists]
  }

  final case class ListOfStructsOfLists(list: List[ListOfPrimitives])
  object ListOfStructsOfLists {
    implicit val schema: Schema[ListOfStructsOfLists] = DeriveSchema.gen[ListOfStructsOfLists]
  }

}
