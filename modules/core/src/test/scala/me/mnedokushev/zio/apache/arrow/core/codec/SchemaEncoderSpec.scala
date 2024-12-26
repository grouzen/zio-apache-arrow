package me.mnedokushev.zio.apache.arrow.core.codec

import me.mnedokushev.zio.apache.arrow.core.Fixtures._
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.{ ArrowType, Field, Schema => JSchema }
import zio.Scope
import zio.schema._
import zio.test.Assertion._
import zio.test.{ Spec, _ }

import scala.jdk.CollectionConverters._

object SchemaEncoderSpec extends ZIOSpecDefault {

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("SchemaEncoder")(
      encodeFlatSpec
    )

  final case class Summoned(a: Int, b: Double, c: String)
  object Summoned {
    implicit val schema: Schema[Summoned]               =
      DeriveSchema.gen[Summoned]
    implicit val intSchemaEncoder: SchemaEncoder[Int]   =
      SchemaEncoder.primitive[Int] { case (name, nullable) =>
        SchemaEncoder.primitiveField(name, new ArrowType.Int(64, true), nullable)
      }
    // TODO: fix fromSummonedDeriver
    // implicit val schemaEncoder: SchemaEncoder[Summoned] =
    //   SchemaEncoder.fromSummonedDeriver[Summoned]
    // implicit val schemaEncoder: SchemaEncoder[Summoned] =
    //   SchemaEncoder.encoder[Summoned](SchemaEncoderDeriver.summoned)
    implicit val schemaEncoder: SchemaEncoder[Summoned] =
      Derive.derive[SchemaEncoder, Summoned](SchemaEncoderDeriver.summoned)
  }

  val encodeFlatSpec: Spec[Any, Throwable] =
    suite("schemaRoot")(
      test("primitive") {
        for {
          result <- Primitives.schemaEncoder.encode
          fields  = getFields(result)
        } yield assert(fields)(
          contains(SchemaEncoder.primitiveField("a", new ArrowType.Int(32, true), nullable = false))
        ) &&
          assert(fields)(
            contains(
              SchemaEncoder
                .primitiveField("b", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), nullable = false)
            )
          ) &&
          assert(fields)(contains(SchemaEncoder.primitiveField("c", new ArrowType.Utf8, nullable = false)))
      },
      test("struct") {
        for {
          result <- StructOfPrimitives.schemaEncoder.encode
          fields  = getFields(result)
        } yield assert(fields)(
          contains(
            SchemaEncoder.structField(
              "struct",
              List(
                SchemaEncoder.primitiveField("a", new ArrowType.Int(32, true), nullable = false),
                SchemaEncoder.primitiveField(
                  "b",
                  new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE),
                  nullable = false
                ),
                SchemaEncoder.primitiveField("c", new ArrowType.Utf8, nullable = false)
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
            SchemaEncoder.listField(
              "list",
              SchemaEncoder.primitiveField("element", new ArrowType.Int(32, true), nullable = false),
              nullable = false
            )
          )
        )
      },
      test("nullable primitives") {
        for {
          result <- NullablePrimitives.schemaEncoder.encode
          fields  = getFields(result)
        } yield assert(fields)(
          contains(SchemaEncoder.primitiveField("a", new ArrowType.Int(32, true), nullable = true))
        ) &&
          assert(fields)(
            contains(
              SchemaEncoder
                .primitiveField("b", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), nullable = true)
            )
          )
      },
      test("summoned") {
        for {
          result <- Summoned.schemaEncoder.encode
          fields  = getFields(result)
        } yield assert(fields)(
          contains(SchemaEncoder.primitiveField("a", new ArrowType.Int(64, true), nullable = false))
        ) &&
          assert(fields)(
            contains(
              SchemaEncoder.primitiveField(
                "b",
                new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE),
                nullable = false
              )
            )
          ) &&
          assert(fields)(contains(SchemaEncoder.primitiveField("c", new ArrowType.Utf8, nullable = false)))
      }
    )

  private def getFields(schema: JSchema): List[Field] =
    schema.getFields.asScala.toList

}
