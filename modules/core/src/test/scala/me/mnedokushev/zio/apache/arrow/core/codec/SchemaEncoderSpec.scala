package me.mnedokushev.zio.apache.arrow.core.codec

import me.mnedokushev.zio.apache.arrow.core.Allocator
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

object SchemaEncoderSpec extends ZIOSpecDefault {

  import SchemaEncoder._

  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("SchemaEncoder")(
      encodeFlatSpec
    )

  val encodeFlatSpec =
    suite("encodeFlat")(
      test("primitive") {
        for {
          result <- schemaRoot[Primitives]
          fields  = getFields(result)
        } yield assert(fields)(contains(fieldNotNullable("a", new ArrowType.Int(32, true)))) &&
          assert(fields)(contains(fieldNotNullable("b", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)))) &&
          assert(fields)(contains(fieldNotNullable("c", new ArrowType.Utf8)))
      },
      test("struct") {
        for {
          result <- schemaRoot[StructOfPrimitives]
          fields  = getFields(result)
        } yield assert(fields)(contains(fieldNotNullable("struct", new ArrowType.Struct)))
      },
      test("list") {
        for {
          result <- schemaRoot[ListOfPrimitives]
          fields  = getFields(result)
        } yield assert(fields)(contains(fieldNotNullable("list", new ArrowType.List)))
      },
      test("nullable primitives") {
        for {
          result <- schemaRoot[NullablePrimitives]
          fields  = getFields(result)
        } yield assert(fields)(contains(fieldNullable("a", new ArrowType.Int(32, true)))) &&
          assert(fields)(contains(fieldNullable("b", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))))
      }
    )

  private def getFields(schema: Schema): List[Field] =
    schema.getFields.asScala.toList

}
