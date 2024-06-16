package me.mnedokushev.zio.apache.arrow.core

import me.mnedokushev.zio.apache.arrow.core.codec.{ SchemaEncoder, SchemaEncoderDeriver }
import zio.schema._
import zio.schema.Factory._
import me.mnedokushev.zio.apache.arrow.core.codec.VectorSchemaRootDecoder
import me.mnedokushev.zio.apache.arrow.core.codec.VectorSchemaRootCodec

object Fixtures {

  final case class Primitives(a: Int, b: Double, c: String)
  object Primitives {
    implicit val schema: Schema[Primitives]                                   =
      DeriveSchema.gen[Primitives]
    implicit val schemaEncoder: SchemaEncoder[Primitives]                     =
      Derive.derive[SchemaEncoder, Primitives](SchemaEncoderDeriver.default)
    implicit val deriverFactory: Factory[Primitives]                          = factory[Primitives]
    implicit val vectorSchemaRootDecoder: VectorSchemaRootDecoder[Primitives] =
      VectorSchemaRootDecoder[Primitives]
    implicit val vectorSchemaRootCodec: VectorSchemaRootCodec[Primitives]     =
      VectorSchemaRootCodec.codec[Primitives]

  }

  // TODO: implement deriveOption
  // final case class NullablePrimitives(a: Option[Int], b: Option[Double])
  // object NullablePrimitives {
  //   implicit val schema: Schema[NullablePrimitives]                                   =
  //     DeriveSchema.gen[NullablePrimitives]
  //   implicit val schemaEncoder: SchemaEncoder[NullablePrimitives]                     =
  //     Derive.derive[SchemaEncoder, NullablePrimitives](SchemaEncoderDeriver.default)
  //   implicit val deriverFactory: Factory[NullablePrimitives]                          = factory[NullablePrimitives]
  //   implicit val vectorSchemaRootDecoder: VectorSchemaRootDecoder[NullablePrimitives] =
  //     VectorSchemaRootDecoder[NullablePrimitives]
  // }

  final case class StructOfPrimitives(struct: Primitives)
  object StructOfPrimitives {
    implicit val schema: Schema[StructOfPrimitives]               =
      DeriveSchema.gen[StructOfPrimitives]
    implicit val schemaEncoder: SchemaEncoder[StructOfPrimitives] =
      Derive.derive[SchemaEncoder, StructOfPrimitives](SchemaEncoderDeriver.default)
  }

  final case class StructOfLists(struct: ListOfPrimitives)
  object StructOfLists {
    implicit val schema: Schema[StructOfLists]               =
      DeriveSchema.gen[StructOfLists]
    implicit val schemaEncoder: SchemaEncoder[StructOfLists] =
      Derive.derive[SchemaEncoder, StructOfLists](SchemaEncoderDeriver.default)
  }

  final case class StructOfStructs(struct: StructOfPrimitives)
  object StructOfStructs {
    implicit val schema: Schema[StructOfStructs]               =
      DeriveSchema.gen[StructOfStructs]
    implicit val schemaEncoder: SchemaEncoder[StructOfStructs] =
      Derive.derive[SchemaEncoder, StructOfStructs](SchemaEncoderDeriver.default)
  }

  final case class StructOfListsOfStructs(struct: ListOfStructs)
  object StructOfListsOfStructs {
    implicit val schema: Schema[StructOfListsOfStructs]               =
      DeriveSchema.gen[StructOfListsOfStructs]
    implicit val schemaEncoder: SchemaEncoder[StructOfListsOfStructs] =
      Derive.derive[SchemaEncoder, StructOfListsOfStructs](SchemaEncoderDeriver.default)
  }

  final case class ListOfPrimitives(list: List[Int])
  object ListOfPrimitives {
    implicit val schema: Schema[ListOfPrimitives]               =
      DeriveSchema.gen[ListOfPrimitives]
    implicit val schemaEncoder: SchemaEncoder[ListOfPrimitives] =
      Derive.derive[SchemaEncoder, ListOfPrimitives](SchemaEncoderDeriver.default)
  }

  final case class ListOfStructs(list: List[Primitives])
  object ListOfStructs {
    implicit val schema: Schema[ListOfStructs]               =
      DeriveSchema.gen[ListOfStructs]
    implicit val schemaEncoder: SchemaEncoder[ListOfStructs] =
      Derive.derive[SchemaEncoder, ListOfStructs](SchemaEncoderDeriver.default)
  }

  final case class ListOfLists(list: List[List[Int]])
  object ListOfLists {
    implicit val schema: Schema[ListOfLists]               =
      DeriveSchema.gen[ListOfLists]
    implicit val schemaEncoder: SchemaEncoder[ListOfLists] =
      Derive.derive[SchemaEncoder, ListOfLists](SchemaEncoderDeriver.default)
  }

  final case class ListOfStructsOfLists(list: List[ListOfPrimitives])
  object ListOfStructsOfLists {
    implicit val schema: Schema[ListOfStructsOfLists]               =
      DeriveSchema.gen[ListOfStructsOfLists]
    implicit val schemaEncoder: SchemaEncoder[ListOfStructsOfLists] =
      Derive.derive[SchemaEncoder, ListOfStructsOfLists](SchemaEncoderDeriver.default)
  }

}
