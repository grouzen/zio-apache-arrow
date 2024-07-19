package me.mnedokushev.zio.apache.arrow.core.codec

import me.mnedokushev.zio.apache.arrow.core.Fixtures._
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.{ ArrowType, Field, Schema => JSchema }
import zio.Scope
import zio.schema._
import zio.schema.Factory._
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
          SchemaEncoder.primitive(name, new ArrowType.Int(64, true), nullable)
      }
    // TODO: fix fromSummonedDeriver
    // implicit val schemaEncoder: SchemaEncoder[Summoned] =
    //   SchemaEncoder.fromSummonedDeriver[Summoned]
    implicit val schemaEncoder: SchemaEncoder[Summoned] =
      Derive.derive[SchemaEncoder, Summoned](SchemaEncoderDeriver.summoned)
  }

  val encodeFlatSpec: Spec[Any, Throwable] =
    suite("schemaRoot")(
      test("primitive") {
        for {
          result <- Primitives.schemaEncoder.encode
          fields  = getFields(result)
        } yield assert(fields)(contains(SchemaEncoder.primitive("a", new ArrowType.Int(32, true), nullable = false))) &&
          assert(fields)(
            contains(
              SchemaEncoder.primitive("b", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), nullable = false)
            )
          ) &&
          assert(fields)(contains(SchemaEncoder.primitive("c", new ArrowType.Utf8, nullable = false)))
      },
      test("struct") {
        for {
          result <- StructOfPrimitives.schemaEncoder.encode
          fields  = getFields(result)
        } yield assert(fields)(
          contains(
            SchemaEncoder.struct(
              "struct",
              List(
                SchemaEncoder.primitive("a", new ArrowType.Int(32, true), nullable = false),
                SchemaEncoder.primitive(
                  "b",
                  new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE),
                  nullable = false
                ),
                SchemaEncoder.primitive("c", new ArrowType.Utf8, nullable = false)
              ),
              nullable = false
            )
          )
        )
      },
      test("list") {
        for {
          result <- ListOfPrimitives.schemaEncoder.encode
          fields  = getFields(result)
        } yield assert(fields)(
          contains(
            SchemaEncoder.list(
              "list",
              SchemaEncoder.primitive("element", new ArrowType.Int(32, true), nullable = false),
              nullable = false
            )
          )
        )
      },
      test("nullable primitives") {
        for {
          result <- NullablePrimitives.schemaEncoder.encode
          fields  = getFields(result)
        } yield assert(fields)(contains(SchemaEncoder.primitive("a", new ArrowType.Int(32, true), nullable = true))) &&
          assert(fields)(
            contains(
              SchemaEncoder.primitive("b", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), nullable = true)
            )
          )
      },
      test("summoned") {
        for {
          result <- Summoned.schemaEncoder.encode
          fields  = getFields(result)
        } yield assert(fields)(contains(SchemaEncoder.primitive("a", new ArrowType.Int(64, true), nullable = false))) &&
          assert(fields)(
            contains(
              SchemaEncoder.primitive("b", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), nullable = false)
            )
          ) &&
          assert(fields)(contains(SchemaEncoder.primitive("c", new ArrowType.Utf8, nullable = false)))
      }
    )

  private def getFields(schema: JSchema): List[Field] =
    schema.getFields.asScala.toList

}
