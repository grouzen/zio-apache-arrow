import sbt.*
import sbt.Keys.scalaVersion

object Dep {

  object V {
    val zio       = "2.0.13"
    val zioSchema = "0.4.11"
  }

  object O {
    val apacheArrow = "org.apache.arrow"
    val scalaLang   = "org.scala-lang"
    val zio         = "dev.zio"
  }

  lazy val arrowFormat = O.apacheArrow % "arrow-format" % "0.17.1"
  lazy val arrowVector = O.apacheArrow % "arrow-vector" % "0.17.1"
  lazy val arrowMemory = O.apacheArrow % "arrow-memory" % "0.17.1"

  lazy val zio                 = O.zio %% "zio"                   % V.zio
  lazy val zioSchema           = O.zio %% "zio-schema"            % V.zioSchema
  lazy val zioSchemaDerivation = O.zio %% "zio-schema-derivation" % V.zioSchema
  lazy val zioTest             = O.zio %% "zio-test"              % V.zio
  lazy val zioTestSbt          = O.zio %% "zio-test-sbt"          % V.zio

  lazy val core = Seq(
    arrowFormat,
    arrowVector,
    arrowMemory,
    zio,
    zioSchema,
    zioSchemaDerivation % Test,
    zioTest             % Test,
    zioTestSbt          % Test
  )

}
