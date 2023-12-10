![Build status](https://github.com/grouzen/zio-apache-arrow/actions/workflows/ci.yml/badge.svg)
![Sonatype Nexus (Releases)](https://img.shields.io/nexus/r/me.mnedokushev/zio-apache-arrow-core_2.12?server=https%3A%2F%2Foss.sonatype.org)
![Sonatype Nexus (Snapshots)](https://img.shields.io/nexus/s/me.mnedokushev/zio-apache-arrow-core_2.13?server=https%3A%2F%2Foss.sonatype.org)
[![Scala Steward badge](https://img.shields.io/badge/Scala_Steward-helping-blue.svg?style=flat&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAA4AAAAQCAMAAAARSr4IAAAAVFBMVEUAAACHjojlOy5NWlrKzcYRKjGFjIbp293YycuLa3pYY2LSqql4f3pCUFTgSjNodYRmcXUsPD/NTTbjRS+2jomhgnzNc223cGvZS0HaSD0XLjbaSjElhIr+AAAAAXRSTlMAQObYZgAAAHlJREFUCNdNyosOwyAIhWHAQS1Vt7a77/3fcxxdmv0xwmckutAR1nkm4ggbyEcg/wWmlGLDAA3oL50xi6fk5ffZ3E2E3QfZDCcCN2YtbEWZt+Drc6u6rlqv7Uk0LdKqqr5rk2UCRXOk0vmQKGfc94nOJyQjouF9H/wCc9gECEYfONoAAAAASUVORK5CYII=)](https://scala-steward.org)

# ZIO Apache Arrow

ZIO based wrapper for [Apache Arrow Java Implementation](https://arrow.apache.org/docs/java/index.html) that leverages
[ZIO Schema](https://zio.dev/zio-schema/) library to derive codecs
for [ValueVector](https://arrow.apache.org/docs/java/reference/index.html)
and [VectorSchemaRoot](https://arrow.apache.org/docs/java/reference/index.html)

# Overview

## Installation

```scala
libraryDependencies += "me.mnedokushev" %% "zio-apache-arrow-core" % "@VERSION@"
```

## Codecs

```scala
val codec = ValueVectorCodec[Int, IntVector]

ZIO.scoped(
  for {
    vec <- codec.encodeZIO(Chunk(1, 2, 3))
    result <- codec.decodeZIO(vec)
  } yield result
)
```
