package me.mnedokushev.zio.apache.arrow.core.codec

import me.mnedokushev.zio.apache.arrow.core.ArrowAllocator
import me.mnedokushev.zio.apache.arrow.core.codec.Fixtures.{
  ListOfPrimitives,
  NullablePrimitives,
  Primitives,
  StructOfPrimitives
}
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.{ ArrowType, Field, FieldType, Schema }
import zio.Scope
import zio.test.Assertion._
import zio.test._

import scala.jdk.CollectionConverters._

object ArrowSchemaEncoderSpec extends ZIOSpecDefault {

  import ArrowSchemaEncoder._

  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("ArrowSchemaEncoder")(
      encodeFlatSpec
    )

  val encodeFlatSpec =
    suite("encodeFlat")(
      test("primitive") {
        for {
          result <- encodeFlat[Primitives]
          fields  = getFields(result)
        } yield assert(fields)(contains(fieldNotNullable("a", new ArrowType.Int(32, true)))) &&
          assert(fields)(contains(fieldNotNullable("b", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)))) &&
          assert(fields)(contains(fieldNotNullable("c", new ArrowType.Utf8)))
      },
      test("struct") {
        for {
          result <- encodeFlat[StructOfPrimitives]
          fields  = getFields(result)
        } yield assert(fields)(contains(fieldNotNullable("struct", new ArrowType.Struct)))
      },
      test("list") {
        for {
          result <- encodeFlat[ListOfPrimitives]
          fields  = getFields(result)
        } yield assert(fields)(contains(fieldNotNullable("list", new ArrowType.List)))
      },
      test("nullable primitives") {
        for {
          result <- encodeFlat[NullablePrimitives]
          fields  = getFields(result)
        } yield assert(fields)(contains(fieldNullable("a", new ArrowType.Int(32, true)))) &&
          assert(fields)(contains(fieldNullable("b", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))))
      }
    )

  private def getFields(schema: Schema): List[Field] =
    schema.getFields.asScala.toList

}
