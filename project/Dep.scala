import sbt._

object Dep {

  object V {
    val zio = "2.0.13"
  }

  object O {
    val apacheArrow = "org.apache.arrow"
    val zio         = "dev.zio"
  }

  lazy val arrowFormat = O.apacheArrow % "arrow-format" % "0.17.1"
  lazy val arrowVector = O.apacheArrow % "arrow-vector" % "0.17.1"
  lazy val arrowMemory = O.apacheArrow % "arrow-memory" % "0.17.1"

  lazy val zio       = O.zio %% "zio"        % V.zio
  lazy val zioSchema = O.zio %% "zio-schema" % "0.4.11"

  lazy val core = Seq(
    arrowFormat,
    arrowVector,
    arrowMemory,
    zio,
    zioSchema
  )

}
