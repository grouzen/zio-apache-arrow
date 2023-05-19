import BuildHelper._

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
      libraryDependencies := Dep.core,
      testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
    )
