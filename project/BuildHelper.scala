import sbt._
import sbt.Keys._
import scalafix.sbt.ScalafixPlugin.autoImport.scalafixSemanticdb

object BuildHelper {

  def stdSettings(projectName: String): Seq[Def.Setting[_]] = Seq(
    name              := s"zio-apache-arrow-$projectName",
    organization      := "me.mnedokushev",
    libraryDependencies ++= betterMonadicFor(scalaVersion.value),
    libraryDependencies ++= kindProjector(scalaVersion.value),
    scalacOptions --= disableUnusedImportsWarnings(scalaVersion.value),
    scalacOptions ++= source3Compatibility(scalaVersion.value),
    semanticdbEnabled := true,
    semanticdbVersion := scalafixSemanticdb.revision,
    Test / javaOptions ++= arrowJavaCompat,
    Test / fork       := true
  )

  val Scala212 = "2.12.20"
  val Scala213 = "2.13.16"
  val Scala3   = "3.4.3"

  private def betterMonadicFor(scalaVersion: String) =
    CrossVersion.partialVersion(scalaVersion) match {
      case Some((2, _)) => Seq(compilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1"))
      case _            => Seq()
    }

  private def kindProjector(scalaVersion: String) =
    CrossVersion.partialVersion(scalaVersion) match {
      case Some((2, _)) => Seq(compilerPlugin("org.typelevel" % "kind-projector" % "0.13.3" cross CrossVersion.full))
      case _            => Seq()
    }

  private def arrowJavaCompat =
    if (System.getProperty("java.version").startsWith("1.8"))
      Seq()
    else
      Seq("--add-opens=java.base/java.nio=ALL-UNNAMED")

  // TODO: can't figure out why scala 2.13 emits 'unused import' for zio.schema.Derive in ValueVectorDecoder.scala
  private def disableUnusedImportsWarnings(scalaVersion: String) =
    CrossVersion.partialVersion(scalaVersion) match {
      case Some((2, 13)) => Seq("-Wunused:imports")
      case _             => Seq()
    }

  // See https://www.scala-lang.org/api/3.x/docs/docs/reference/changed-features/vararg-splices.html#compatibility-considerations-2
  private def source3Compatibility(scalaVersion: String) =
    CrossVersion.partialVersion(scalaVersion) match {
      case Some((2, _)) => Seq("-Xsource:3")
      case _            => Seq()
    }

}
