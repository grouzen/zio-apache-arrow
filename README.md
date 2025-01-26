![Build status](https://github.com/grouzen/zio-apache-arrow/actions/workflows/ci.yml/badge.svg)
![Sonatype Nexus (Releases)](https://img.shields.io/nexus/r/me.mnedokushev/zio-apache-arrow-core_2.12?server=https%3A%2F%2Foss.sonatype.org)
![Sonatype Nexus (Snapshots)](https://img.shields.io/nexus/s/me.mnedokushev/zio-apache-arrow-core_2.13?server=https%3A%2F%2Foss.sonatype.org)
[![Scala Steward badge](https://img.shields.io/badge/Scala_Steward-helping-blue.svg?style=flat&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAA4AAAAQCAMAAAARSr4IAAAAVFBMVEUAAACHjojlOy5NWlrKzcYRKjGFjIbp293YycuLa3pYY2LSqql4f3pCUFTgSjNodYRmcXUsPD/NTTbjRS+2jomhgnzNc223cGvZS0HaSD0XLjbaSjElhIr+AAAAAXRSTlMAQObYZgAAAHlJREFUCNdNyosOwyAIhWHAQS1Vt7a77/3fcxxdmv0xwmckutAR1nkm4ggbyEcg/wWmlGLDAA3oL50xi6fk5ffZ3E2E3QfZDCcCN2YtbEWZt+Drc6u6rlqv7Uk0LdKqqr5rk2UCRXOk0vmQKGfc94nOJyQjouF9H/wCc9gECEYfONoAAAAASUVORK5CYII=)](https://scala-steward.org)

# ZIO Apache Arrow

ZIO based wrapper for [Apache Arrow Java Implementation](https://arrow.apache.org/docs/java/index.html) that leverages
[ZIO Schema](https://zio.dev/zio-schema/) library to derive codecs for [ValueVector](https://arrow.apache.org/docs/java/reference/index.html) and [VectorSchemaRoot](https://arrow.apache.org/docs/java/reference/index.html).

**To the best of my knowledge at the time of writing, this is the only library that provides automatic codecs derivation for Apache Arrow among all type-safe programming languages.**

Want more? Checkout out my ZIO-powered library for Apache Parquet - [ZIO Apache Parquet](https://github.com/grouzen/zio-apache-parquet).

## Why?

- **ZIO native** - utilizes various ZIO features to offer a FP-oriented way of working with the Arrow API.
- **Resource management** - guarantees an automatic [memory management](https://arrow.apache.org/docs/java/memory.html) by wrapping up operations on [BufferAllocator](https://arrow.apache.org/docs/java/memory.html#bufferallocator) in ZIO's [Scope](https://zio.dev/reference/resource/scope/).
- **ZIO Schema** - automatically derives codecs for Arrow's ValueVector and VectorSchemaRoot data types, enabling a seamless integration with the rest of the ZIO ecosystem.
- **Integration with Arrow ecosystem** - the [Core](#core) module provides the groundwork for integrating your application or library with various parts of the Arrow ecosystem such as [Datafusion](https://datafusion.apache.org), [Polars](https://pola.rs), [Apache Iceberg](https://iceberg.apache.org), and more.


## Contents

- [Installation](#installation)
- [Usage](#usage)
  - [Core](#core)
    - [Codecs](#codecs)
      - [ValueVector](#valuevector)
        - [Built-in primitive and complex types](#built-in-primitive-and-complex-types)
        - [Nullable](#nullable)
      - [Tabular](#tabular)
        - [SchemaEncoder](#schemaencoder)
        - [VectorSchemaRoot](#vectorschemaroot)
    - [IPC](#ipc)
  - [Datafusion](#datafusion)

## Installation

```scala
libraryDependencies += "me.mnedokushev" %% "zio-apache-arrow-core"       % "@VERSION@"
libraryDependencies += "me.mnedokushev" %% "zio-apache-arrow-datafusion" % "@VERSION@"
```

## Usage

All examples are self-contained [Scala CLI](https://scala-cli.virtuslab.org) snippets. You can find copies of them in `docs/scala-cli`.

### Core

The module does all the grunt work to implement a familiar FP-oriented API for some of the basic blocks of the Java API of Apache Arrow such as:
- [Memory Management](https://arrow.apache.org/docs/java/memory.html)
- [Value Vector](https://arrow.apache.org/docs/java/vector.html)
- [Tabular data](https://arrow.apache.org/docs/java/vector_schema_root.html)
- [Reading/Writing IPC formats](https://arrow.apache.org/docs/java/ipc.html)

#### Codecs

It helps you to avoid dealing with a low-level, convoluted and erorr-prone Java API by providing codecs for a [bunch of primitive](https://zio.dev/zio-schema/standard-type-reference), arbitrary nested complex ([lists](https://arrow.apache.org/docs/format/Columnar.html#variable-size-list-layout) and [structs](https://arrow.apache.org/docs/format/Columnar.html#struct-layout)), nullable, and user-defined tabular types.

##### ValueVector

https://arrow.apache.org/docs/java/vector.html
> an abstraction that is used to store a sequence of values having the same type in an individual column.

###### Built-in primitive and complex types

```scala
//> using scala "3.4.3"
//> using dep me.mnedokushev::zio-apache-arrow-core:0.1.4
//> using dep org.apache.arrow:arrow-memory-unsafe:18.1.0
//> using javaOpt --add-opens=java.base/java.nio=ALL-UNNAMED

import me.mnedokushev.zio.apache.arrow.core.Allocator
import me.mnedokushev.zio.apache.arrow.core.codec.*
import zio.*
import zio.schema.*
import zio.schema.Factory.*

object ValueVectorCodecs extends ZIOAppDefault:

  case class MyData(a: Int, b: String)

  object MyData:
    implicit val schema: Schema[MyData] = DeriveSchema.gen[MyData]

  val intCodec          = ValueVectorCodec.intCodec
  val listStringCodec   = ValueVectorCodec.listChunkCodec[String]
  val structMyDataCodec = ValueVectorCodec.structCodec[MyData]

  override def run =
    ZIO
      .scoped(
        for {
          intVec             <- intCodec.encodeZIO(Chunk(1, 2, 3))
          intResult          <- intCodec.decodeZIO(intVec)
          listStringVec      <- listStringCodec.encodeZIO(Chunk(Chunk("a", "b"), Chunk("c")))
          listStringResult   <- listStringCodec.decodeZIO(listStringVec)
          structMyDataVec    <- structMyDataCodec.encodeZIO(Chunk(MyData(1, "a"), MyData(2, "b")))
          structMyDataResult <- structMyDataCodec.decodeZIO(structMyDataVec)
          _                  <- Console.printLine(intVec)
          _                  <- Console.printLine(intResult)
          _                  <- Console.printLine(listStringVec)
          _                  <- Console.printLine(listStringResult)
          _                  <- Console.printLine(structMyDataVec)
          _                  <- Console.printLine(structMyDataResult)
        } yield ()
      )
      .provide(Allocator.rootLayer())
  // Outputs:
  // [1, 2, 3]
  // Chunk(1,2,3)
  // [["a","b"], ["c"]]
  // Chunk(Chunk(a,b),Chunk(c))
  // [{"a":1,"b":"a"}, {"a":2,"b":"b"}]
  // Chunk(MyData(1,a),MyData(2,b))
```

###### Nullable

```scala
//> using scala "3.4.3"
//> using dep me.mnedokushev::zio-apache-arrow-core:0.1.4
//> using dep org.apache.arrow:arrow-memory-unsafe:18.1.0
//> using javaOpt --add-opens=java.base/java.nio=ALL-UNNAMED

import me.mnedokushev.zio.apache.arrow.core.Allocator
import me.mnedokushev.zio.apache.arrow.core.codec.*
import org.apache.arrow.vector.IntVector
import zio.*
import zio.schema.*
import zio.schema.Factory.*

object ValueVectorCodecsNullable extends ZIOAppDefault:

  val intCodec              = ValueVectorCodec.optionCodec[IntVector, Int]
  val listStringCodec       = ValueVectorCodec.listChunkOptionCodec[String]
  val optionListStringCodec = ValueVectorCodec.optionListChunkCodec[String]

  override def run =
    ZIO
      .scoped(
        for {
          intVec                 <- intCodec.encodeZIO(Chunk(Some(1), None, Some(2)))
          intResult              <- intCodec.decodeZIO(intVec)
          listStringVec          <- listStringCodec.encodeZIO(Chunk(Chunk(Some("a"), None), Chunk(Some("b"))))
          listStringResult       <- listStringCodec.decodeZIO(listStringVec)
          optionListStringVec    <- optionListStringCodec.encodeZIO(Chunk(Some(Chunk("a", "b")), None, Some(Chunk("c"))))
          optionListStringResult <- optionListStringCodec.decodeZIO(optionListStringVec)
          _                      <- Console.printLine(intVec)
          _                      <- Console.printLine(intResult)
          _                      <- Console.printLine(listStringVec)
          _                      <- Console.printLine(listStringResult)
          _                      <- Console.printLine(optionListStringVec)
          _                      <- Console.printLine(optionListStringResult)
        } yield ()
      )
      .provide(Allocator.rootLayer())
  // Outputs:
  // [1, null, 2]
  // Chunk(Some(1),None,Some(2))
  // [["a",null], ["b"]]
  // Chunk(Chunk(Some(a),None),Chunk(None))
  // [["a","b"], null, ["c"]]
  // Chunk(Some(Chunk(a,b)),None,Some(Chunk(c)))
```

##### Tabular

https://arrow.apache.org/docs/java/vector_schema_root.html
> The recommended usage is to create a single VectorSchemaRoot based on a known schema and populate data over and over into that root in a stream of batches, rather than creating a new instance each time.

The API is similar to the ValueVectorCodec. The main difference is that it is supposed to be used with user-defined case classes (aka 2D datasets) only.

###### SchemaEncoder

> describes the overall structure consisting of any number of columns. It holds a sequence of fields together with some optional schema-wide metadata (in addition to per-field metadata).

When working with tabular data, we need a way to convert ZIO Schema into [Arrow Schema](https://arrow.apache.org/docs/java/reference/org/apache/arrow/vector/types/pojo/Schema.html).

```scala
//> using scala "3.4.3"
//> using dep me.mnedokushev::zio-apache-arrow-core:0.1.4
//> using dep org.apache.arrow:arrow-memory-unsafe:18.1.0
//> using javaOpt --add-opens=java.base/java.nio=ALL-UNNAMED

import me.mnedokushev.zio.apache.arrow.core.codec.*
import zio.*
import zio.schema.*

object TabularSchemaEncoder extends ZIOAppDefault:

  case class MyData(a: Int, b: String)

  object MyData:
    implicit val schema: Schema[MyData]               =
      DeriveSchema.gen[MyData]
    implicit val schemaEncoder: SchemaEncoder[MyData] =
      SchemaEncoder.fromDefaultDeriver[MyData]

  override def run =
    for {
      schema <- ZIO.fromEither(MyData.schemaEncoder.encode(MyData.schema))
      _      <- Console.printLine(schema)
    } yield ()
  // Outputs:
  // Schema<a: Int(64, true) not null, b: Utf8 not null>
```

###### VectorSchemaRoot

```scala
//> using scala "3.4.3"
//> using dep me.mnedokushev::zio-apache-arrow-core:0.1.4
//> using dep org.apache.arrow:arrow-memory-unsafe:18.1.0
//> using javaOpt --add-opens=java.base/java.nio=ALL-UNNAMED

import me.mnedokushev.zio.apache.arrow.core.Allocator
import me.mnedokushev.zio.apache.arrow.core.codec.*
import me.mnedokushev.zio.apache.arrow.core.Tabular.*
import me.mnedokushev.zio.apache.arrow.core.Tabular
import zio.*
import zio.schema.*
import zio.schema.Factory.*

object TabularVectorSchemaRoot extends ZIOAppDefault:

  case class MyData(a: Int, b: String)

  object MyData:
    implicit val schema: Schema[MyData]               =
      DeriveSchema.gen[MyData]
    implicit val schemaEncoder: SchemaEncoder[MyData] =
      SchemaEncoder.fromDefaultDeriver[MyData]

  val myDataCodec = VectorSchemaRootCodec.codec[MyData]

  override def run =
    ZIO
      .scoped(
        for {
          root         <- Tabular.empty[MyData]
          myDataVec    <- myDataCodec.encodeZIO(Chunk(MyData(1, "a"), MyData(2, "b")), root)
          myDataResult <- myDataCodec.decodeZIO(myDataVec)
          _            <- Console.printLine(myDataResult)
        } yield ()
      )
      .provide(Allocator.rootLayer())
  // Outputs:
  // Chunk(MyData(1,a),MyData(2,b))
```

You also can use methods from `Tabular` to simplify encoding/decoding:

```scala
ZIO.scoped(
  for {
    myDataVec    <- Tabular.fromChunk(Chunk(MyData(1, "a"), MyData(2, "b")))
    myDataResult <- Tabular.toChunk(myDataVec)
  } yield ()
).provide(Allocator.rootLayer())
```

#### IPC

https://arrow.apache.org/docs/java/ipc.html
> Arrow defines two types of binary formats for serializing record batches:
> - Streaming format: for sending an arbitrary number of record batches. The format must be processed from start to end, and does not support random access
> - File or Random Access format: for serializing a fixed number of record batches. It supports random access, and thus is very useful when used with memory maps

Now, knowing how to define codecs we may use this knowledge to make some real-world serialization/deserialization:

```scala
//> using scala "3.4.3"
//> using dep me.mnedokushev::zio-apache-arrow-core:0.1.4
//> using dep org.apache.arrow:arrow-memory-unsafe:18.1.0
//> using javaOpt --add-opens=java.base/java.nio=ALL-UNNAMED

import me.mnedokushev.zio.apache.arrow.core.Allocator
import me.mnedokushev.zio.apache.arrow.core.codec.*
import me.mnedokushev.zio.apache.arrow.core.ipc.*
import java.io.ByteArrayInputStream
import zio.*
import zio.stream.*
import zio.schema.*
import zio.schema.Factory.*

object IPC extends ZIOAppDefault:

  case class MyData(a: Int, b: String)

  object MyData:
    implicit val schema: Schema[MyData]                   =
      DeriveSchema.gen[MyData]
    implicit val schemaEncoder: SchemaEncoder[MyData]     =
      SchemaEncoder.fromDefaultDeriver[MyData]
    implicit val encoder: VectorSchemaRootDecoder[MyData] =
      VectorSchemaRootDecoder.fromDefaultDeriver[MyData]
    implicit val decoder: VectorSchemaRootEncoder[MyData] =
      VectorSchemaRootEncoder.fromDefaultDeriver[MyData]

  val payload = (1 to 8096).map(i => MyData(i, s"string ${i.toString}"))

  override def run =
    ZIO
      .scoped(
        for {
          out    <- writeStreaming[Any, MyData](ZStream.from(payload))
          result <- readStreaming[MyData](new ByteArrayInputStream(out.toByteArray)).runCollect
          _      <- Console.printLine(result)
        } yield ()
      )
      .provide(Allocator.rootLayer())
  // Outputs:
  // Chunk(MyData(1,string 1), ..., MyData(8096,string 8096))
```

### Datafusion

This module provides a thin wrapper around [Apache Arrow's DataFusion library](https://github.com/G-Research/datafusion-java). It enables running SQL queries on data loaded from CSV or Parquet files, and then processing the results using the power of ZIO and ZIO Streams.

For this example we will use the following CSV file:
```csv
fname,lname,address,age
Bob,Dylan,Hollywood,80
Dog,Cat,NY,3
John,Doe,London,99
```

```scala
//> using scala "3.4.3"
//> using dep me.mnedokushev::zio-apache-arrow-core:0.1.4
//> using dep me.mnedokushev::zio-apache-arrow-datafusion:0.1.4
//> using dep org.apache.arrow:arrow-memory-unsafe:18.1.0
//> using javaOpt --add-opens=java.base/java.nio=ALL-UNNAMED

import me.mnedokushev.zio.apache.arrow.core.Allocator
import me.mnedokushev.zio.apache.arrow.core.codec.*
import me.mnedokushev.zio.apache.arrow.datafusion.*
import zio.*
import zio.schema.*

import java.nio.file.Paths
import java.io.File

object Datafusion extends ZIOAppDefault:

  case class User(fname: String, lname: String, address: String, age: Long)

  object User:
    implicit val schema: Schema[User]                                   =
      DeriveSchema.gen[User]
    implicit val schemaEncoder: SchemaEncoder[User]                     =
      Derive.derive[SchemaEncoder, User](SchemaEncoderDeriver.default)
    implicit val vectorSchemaRootDecoder: VectorSchemaRootDecoder[User] =
      VectorSchemaRootDecoder.fromDefaultDeriver[User]

  override def run =
    (
      ZIO
        .serviceWithZIO[Context] { context =>
          for {
            _      <- context.registerCsv("test", Paths.get(new File("test.csv").toURI))
            df     <- context.sql("SELECT * FROM test WHERE fname = 'Dog'")
            result <- df.collect[User].runCollect
            _      <- Console.printLine(result)
          } yield ()
        }
      )
      .provide(Context.create, Allocator.rootLayer())
  // Outputs:
  // Chunk(User(Dog,Cat,NY,3))
```

You can also write the data back to CSV or Parquet files using `df.writeCsv` and `df.writeParquet` methods.