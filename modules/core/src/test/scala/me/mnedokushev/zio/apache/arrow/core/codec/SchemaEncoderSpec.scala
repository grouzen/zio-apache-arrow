package me.mnedokushev.zio.apache.arrow.core.codec

import me.mnedokushev.zio.apache.arrow.core.Fixtures._
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.{ ArrowType, Field, Schema }
import zio.Scope
import zio.test.Assertion._
import zio.test.{ Spec, _ }

import scala.jdk.CollectionConverters._

object SchemaEncoderSpec extends ZIOSpecDefault {

  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("SchemaEncoder")(
      encodeFlatSpec
    )

  import SchemaEncoderDeriver._

  val encodeFlatSpec: Spec[Any, Throwable] =
    suite("schemaRoot")(
      test("primitive") {
        for {
          result <- Primitives.schemaEncoder.encode
          fields  = getFields(result)
        } yield assert(fields)(contains(fieldNotNullable("a", new ArrowType.Int(32, true)))) &&
          assert(fields)(contains(fieldNotNullable("b", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)))) &&
          assert(fields)(contains(fieldNotNullable("c", new ArrowType.Utf8)))
      },
      test("struct") {
        for {
          result <- StructOfPrimitives.schemaEncoder.encode
          fields  = getFields(result)
        } yield assert(fields)(contains(fieldNotNullable("struct", new ArrowType.Struct)))
      },
      test("list") {
        for {
          result <- ListOfPrimitives.schemaEncoder.encode
          fields  = getFields(result)
        } yield assert(fields)(contains(fieldNotNullable("list", new ArrowType.List)))
      },
      test("nullable primitives") {
        for {
          result <- NullablePrimitives.schemaEncoder.encode
          fields  = getFields(result)
        } yield assert(fields)(contains(fieldNullable("a", new ArrowType.Int(32, true)))) &&
          assert(fields)(contains(fieldNullable("b", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))))
      }
    )

  private def getFields(schema: Schema): List[Field] =
    schema.getFields.asScala.toList

}
