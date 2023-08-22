import BuildHelper.*
import Dep.O

inThisBuild(
  List(
    name               := "ZIO Apache Arrow",
    organization       := "me.mnedokushev",
    licenses           := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
    developers         := List(
      Developer(
        "grouzen",
        "Mykhailo Nedokushev",
        "michael.nedokushev@gmail.com",
        url("https://github.com/grouzen")
      )
    ),
    scmInfo            := Some(
      ScmInfo(
        url("https://github.com/grouzen/zio-apache-arrow"),
        "scm:git:git@github.com:grouzen/zio-apache-arrow.git"
      )
    ),
    crossScalaVersions := Seq(Scala212, Scala213, Scala3),
    githubWorkflowJavaVersions ++= Seq(JavaSpec.temurin("11"), JavaSpec.temurin("17")),
    githubWorkflowPublishTargetBranches := Seq()
  )
)

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
      libraryDependencies ++= Dep.core,
      testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
    )
