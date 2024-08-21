package me.mnedokushev.zio.apache.arrow.core.codec

import org.apache.arrow.vector.types.pojo.{ ArrowType, Field, FieldType, Schema => JSchema }
import zio.schema.{ Deriver, Factory, Schema, StandardType }

import scala.jdk.CollectionConverters._
import scala.annotation.nowarn

trait SchemaEncoder[A] { self =>

  def encode(implicit schema: Schema[A]): Either[Throwable, JSchema] =
    Left(EncoderError(s"Given ZIO schema $schema mut be of type Schema.Record[A]"))

  def encodeField(name: String, nullable: Boolean): Field

}

object SchemaEncoder {

  def primitive[A](encode0: (String, Boolean) => Field)(implicit @nowarn ev: StandardType[A]): SchemaEncoder[A] =
    new SchemaEncoder[A] {

      override def encodeField(name: String, nullable: Boolean): Field =
        encode0(name, nullable)
    }

  implicit def encoder[A: Factory: Schema](deriver: Deriver[SchemaEncoder]): SchemaEncoder[A] =
    implicitly[Factory[A]].derive[SchemaEncoder](deriver)

  def fromDeriver[A: Factory: Schema](deriver: Deriver[SchemaEncoder]): SchemaEncoder[A] =
    implicitly[Factory[A]].derive[SchemaEncoder](deriver)

  def fromDefaultDeriver[A: Factory: Schema]: SchemaEncoder[A] =
    fromDeriver(SchemaEncoderDeriver.default)

  def fromSummonedDeriver[A: Factory: Schema]: SchemaEncoder[A] =
    fromDeriver(SchemaEncoderDeriver.summoned)

  def primitiveField(name: String, tpe: ArrowType.PrimitiveType, nullable: Boolean): Field =
    new Field(name, new FieldType(nullable, tpe, null), null)

  def listField(name: String, child: Field, nullable: Boolean): Field =
    new Field(name, new FieldType(nullable, new ArrowType.List, null), List(child).asJava)

  def structField(name: String, fields: List[Field], nullable: Boolean): Field =
    new Field(name, new FieldType(nullable, new ArrowType.Struct, null), fields.asJava)

}
