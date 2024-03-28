import sbt._
import sbt.Keys._
import scalafix.sbt.ScalafixPlugin.autoImport.scalafixSemanticdb

object BuildHelper {

  def stdSettings(projectName: String): Seq[Def.Setting[_]] = Seq(
    name              := s"zio-apache-arrow-$projectName",
    organization      := "me.mnedokushev",
    libraryDependencies ++= betterMonadicFor(scalaVersion.value),
    semanticdbEnabled := true,
    semanticdbVersion := scalafixSemanticdb.revision,
    Test / javaOptions ++= arrowJavaCompat,
    Test / fork       := true
  )

  val Scala212 = "2.12.19"
  val Scala213 = "2.13.13"
  val Scala3   = "3.4.1"

  private def betterMonadicFor(scalaVersion: String) =
    CrossVersion.partialVersion(scalaVersion) match {
      case Some((2, _)) => Seq(compilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1"))
      case _            => Seq()
    }

  private def arrowJavaCompat =
    if (System.getProperty("java.version").startsWith("1.8"))
      Seq()
    else
      Seq("--add-opens=java.base/java.nio=ALL-UNNAMED")

}
