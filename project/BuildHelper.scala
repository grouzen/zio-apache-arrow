import sbt._
import sbt.Keys._

object BuildHelper {

  def stdSettings(projectName: String): Seq[Def.Setting[_]] = Seq(
    name         := s"zio-apache-arrow-$projectName",
    organization := "me.mnedokushev",
    libraryDependencies ++= (CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, _)) => Seq(compilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1"))
      case _            => Seq()
    })
  )

  val Scala212 = "2.12.18"
  val Scala213 = "2.13.11"
  val Scala3   = "3.3.0"

}
