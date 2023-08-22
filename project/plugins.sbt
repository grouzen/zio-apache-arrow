// Linting
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.0")
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.11.0")

// Dependencies management
addSbtPlugin("ch.epfl.scala"    % "sbt-missinglink"           % "0.3.5")
addSbtPlugin("com.github.cb372" % "sbt-explicit-dependencies" % "0.2.16")

// Versioning and release
addSbtPlugin("com.eed3si9n"   % "sbt-buildinfo"  % "0.11.0")
addSbtPlugin("com.github.sbt" % "sbt-pgp"        % "2.2.1")
addSbtPlugin("com.github.sbt" % "sbt-dynver"     % "5.0.1")
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype"   % "3.9.21")
addSbtPlugin("com.github.sbt" % "sbt-ci-release" % "1.5.12")
addSbtPlugin("org.typelevel"  % "sbt-tpolecat"   % "0.5.0")

addDependencyTreePlugin
