import BuildHelper._
import Dep.O

lazy val root =
  project
    .in(file("."))
    .aggregate(core)
    .settings(publish / skip := true)

lazy val core =
  project
    .in(file("modules/core"))
    .settings(
      stdSettings("core"),
      libraryDependencies ++= Dep.core ++ Seq(
        O.scalaLang % "scala-reflect" % scalaVersion.value % Provided
      ),
      testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
    )
