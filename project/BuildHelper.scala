import sbt._
import sbt.Keys._

object BuildHelper {

  def stdSettings(projectName: String): Seq[Def.Setting[_]] = Seq(
    name         := s"zio-apache-arrow-$projectName",
    organization := "me.mnedokushev",
    scalaVersion := Scala213,
    libraryDependencies += compilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1"),
    scalacOptions ++= Seq(
      "-Ymacro-annotations"
    )
  )

  private val Scala213 = "2.13.10"

}
