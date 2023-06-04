package me.mnedokushev.zio.apache.arrow.core.codec

import zio.schema._

object Fixtures {

  final case class Scalars(a: Int, b: Double, c: String)
  object Scalars {
    implicit val schema: Schema[Scalars] = DeriveSchema.gen[Scalars]
  }

  final case class StructOfScalars(struct: Scalars)
  object StructOfScalars {
    implicit val schema: Schema[StructOfScalars] = DeriveSchema.gen[StructOfScalars]
  }

  final case class StructOfLists(struct: ListOfScalars)
  object StructOfLists {
    implicit val schema: Schema[StructOfLists] = DeriveSchema.gen[StructOfLists]
  }

  final case class StructOfStructs(struct: StructOfScalars)
  object StructOfStructs {
    implicit val schema: Schema[StructOfStructs] = DeriveSchema.gen[StructOfStructs]
  }

  final case class StructOfListsOfStructs(struct: ListOfStructs)
  object StructOfListsOfStructs {
    implicit val schema: Schema[StructOfListsOfStructs] = DeriveSchema.gen[StructOfListsOfStructs]
  }

  final case class ListOfScalars(list: List[Int])
  object ListOfScalars {
    implicit val schema: Schema[ListOfScalars] = DeriveSchema.gen[ListOfScalars]
  }

  final case class ListOfStructs(list: List[Scalars])
  object ListOfStructs {
    implicit val schema: Schema[ListOfStructs] = DeriveSchema.gen[ListOfStructs]
  }

  final case class ListOfLists(list: List[List[Int]])
  object ListOfLists {
    implicit val schema: Schema[ListOfLists] = DeriveSchema.gen[ListOfLists]
  }

  final case class ListOfStructsOfLists(list: List[ListOfScalars])
  object ListOfStructsOfLists {
    implicit val schema: Schema[ListOfStructsOfLists] = DeriveSchema.gen[ListOfStructsOfLists]
  }

}
