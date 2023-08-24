import BuildHelper.*

inThisBuild(
  List(
    name                                := "ZIO Apache Arrow",
    organization                        := "me.mnedokushev",
    licenses                            := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
    developers                          := List(
      Developer(
        "grouzen",
        "Mykhailo Nedokushev",
        "michael.nedokushev@gmail.com",
        url("https://github.com/grouzen")
      )
    ),
    scmInfo                             := Some(
      ScmInfo(
        url("https://github.com/grouzen/zio-apache-arrow"),
        "scm:git:git@github.com:grouzen/zio-apache-arrow.git"
      )
    ),
    crossScalaVersions                  := Seq(Scala212, Scala213, Scala3),
    githubWorkflowJavaVersions ++= Seq(JavaSpec.temurin("11"), JavaSpec.temurin("17")),
    githubWorkflowPublishTargetBranches := Seq(),
    githubWorkflowBuildPreamble         := Seq(
      WorkflowStep.Sbt(
        List(
          "scalafix --check",
          "scalafmtCheckAll"
        ),
        name = Some("Lint Scala code")
      )
    )
  )
)

lazy val root =
  project
    .in(file("."))
    .aggregate(core, datafusion, docs)
    .settings(publish / skip := true)

lazy val core =
  project
    .in(file("modules/core"))
    .settings(
      stdSettings("core"),
      libraryDependencies ++= Dep.core,
      testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
    )

lazy val datafusion =
  project
    .in(file("modules/datafusion"))
    .dependsOn(core)
    .settings(
      stdSettings("datafusion"),
      libraryDependencies ++= Dep.datafusion,
      testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
    )

lazy val docs =
  project
    .in(file("docs"))
    .dependsOn(core)
    .settings(
      name           := "zio-apache-arrow-docs",
      organization   := "me.mnedokushev",
      publish / skip := true,
      mdocIn         := file("docs/src/main/mdoc"),
      mdocVariables  := Map(
        "VERSION" -> version.value
      )
    )
    .enablePlugins(MdocPlugin)
