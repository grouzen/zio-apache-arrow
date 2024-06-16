package me.mnedokushev.zio.apache.arrow.core.codec

import me.mnedokushev.zio.apache.arrow.core.Fixtures._
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.{ ArrowType, Field, Schema => JSchema }
import zio.Scope
import zio.schema.{ Derive, DeriveSchema, Schema }
import zio.test.Assertion._
import zio.test.{ Spec, _ }

import scala.jdk.CollectionConverters._

object SchemaEncoderSpec extends ZIOSpecDefault {

  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("SchemaEncoder")(
      encodeFlatSpec
    )

  final case class Summoned(a: Int, b: Double, c: String)
  object Summoned {
    implicit val schema: Schema[Summoned]               =
      DeriveSchema.gen[Summoned]
    implicit val intSchemaEncoder: SchemaEncoder[Int]   =
      new SchemaEncoder[Int] {
        override def encodeField(name: String, nullable: Boolean): Field =
          SchemaEncoder.field(name, new ArrowType.Int(64, true), nullable)
      }
    implicit val schemaEncoder: SchemaEncoder[Summoned] =
      Derive.derive[SchemaEncoder, Summoned](SchemaEncoderDeriver.summoned)
  }

  val encodeFlatSpec: Spec[Any, Throwable] =
    suite("schemaRoot")(
      test("primitive") {
        for {
          result <- Primitives.schemaEncoder.encode
          fields  = getFields(result)
        } yield assert(fields)(contains(SchemaEncoder.fieldNotNullable("a", new ArrowType.Int(32, true)))) &&
          assert(fields)(
            contains(SchemaEncoder.fieldNotNullable("b", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)))
          ) &&
          assert(fields)(contains(SchemaEncoder.fieldNotNullable("c", new ArrowType.Utf8)))
      },
      test("struct") {
        for {
          result <- StructOfPrimitives.schemaEncoder.encode
          fields  = getFields(result)
        } yield assert(fields)(contains(SchemaEncoder.fieldNotNullable("struct", new ArrowType.Struct)))
      },
      test("list") {
        for {
          result <- ListOfPrimitives.schemaEncoder.encode
          fields  = getFields(result)
        } yield assert(fields)(contains(SchemaEncoder.fieldNotNullable("list", new ArrowType.List)))
      },
      // TODO: implement deriveOption
      // test("nullable primitives") {
      //   for {
      //     result <- NullablePrimitives.schemaEncoder.encode
      //     fields  = getFields(result)
      //   } yield assert(fields)(contains(SchemaEncoder.fieldNullable("a", new ArrowType.Int(32, true)))) &&
      //     assert(fields)(
      //       contains(SchemaEncoder.fieldNullable("b", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)))
      //     )
      // },
      test("summoned") {
        for {
          result <- Summoned.schemaEncoder.encode
          fields  = getFields(result)
        } yield assert(fields)(contains(SchemaEncoder.fieldNotNullable("a", new ArrowType.Int(64, true)))) &&
          assert(fields)(
            contains(SchemaEncoder.fieldNotNullable("b", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)))
          ) &&
          assert(fields)(contains(SchemaEncoder.fieldNotNullable("c", new ArrowType.Utf8)))
      }
    )

  private def getFields(schema: JSchema): List[Field] =
    schema.getFields.asScala.toList

}
