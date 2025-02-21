import sbt._

object Dep {

  object V {
    val zio                   = "2.1.15"
    val zioSchema             = "1.6.3"
    val arrow                 = "18.2.0"
    val scalaCollectionCompat = "2.13.0"
  }

  object O {
    val apacheArrow      = "org.apache.arrow"
    val scalaLang        = "org.scala-lang"
    val zio              = "dev.zio"
    val scalaLangModules = "org.scala-lang.modules"
  }

  lazy val arrowFormat       = O.apacheArrow % "arrow-format"        % V.arrow
  lazy val arrowVector       = O.apacheArrow % "arrow-vector"        % V.arrow
  lazy val arrowMemory       = O.apacheArrow % "arrow-memory"        % V.arrow
  lazy val arrowMemoryUnsafe = O.apacheArrow % "arrow-memory-unsafe" % V.arrow

  lazy val zio                 = O.zio %% "zio"                   % V.zio
  lazy val zioSchema           = O.zio %% "zio-schema"            % V.zioSchema
  lazy val zioSchemaDerivation = O.zio %% "zio-schema-derivation" % V.zioSchema
  lazy val zioTest             = O.zio %% "zio-test"              % V.zio
  lazy val zioTestSbt          = O.zio %% "zio-test-sbt"          % V.zio

  lazy val scalaCollectionCompat = O.scalaLangModules %% "scala-collection-compat" % V.scalaCollectionCompat

  lazy val datafusionJava = "uk.co.gresearch.datafusion" % "datafusion-java" % "0.12.0"

  lazy val core = Seq(
    arrowFormat,
    arrowVector,
    arrowMemory,
    zio,
    zioSchema,
    zioSchemaDerivation,
    scalaCollectionCompat,
    arrowMemoryUnsafe % Test,
    zioTest           % Test,
    zioTestSbt        % Test
  )

  lazy val datafusion = Seq(
    arrowFormat,
    arrowVector,
    arrowMemory,
    datafusionJava,
    zio,
    zioSchema,
    zioSchemaDerivation,
    scalaCollectionCompat,
    arrowMemoryUnsafe % Test,
    zioTest           % Test,
    zioTestSbt        % Test
  )

}
