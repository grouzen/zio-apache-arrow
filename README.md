![Build status](https://github.com/grouzen/zio-apache-arrow/actions/workflows/ci.yml/badge.svg)
![Sonatype Nexus (Releases)](https://img.shields.io/nexus/r/me.mnedokushev/zio-apache-arrow-core_2.12?server=https%3A%2F%2Foss.sonatype.org)
![Sonatype Nexus (Snapshots)](https://img.shields.io/nexus/s/me.mnedokushev/zio-apache-arrow-core_2.13?server=https%3A%2F%2Foss.sonatype.org)
[![Scala Steward badge](https://img.shields.io/badge/Scala_Steward-helping-blue.svg?style=flat&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAA4AAAAQCAMAAAARSr4IAAAAVFBMVEUAAACHjojlOy5NWlrKzcYRKjGFjIbp293YycuLa3pYY2LSqql4f3pCUFTgSjNodYRmcXUsPD/NTTbjRS+2jomhgnzNc223cGvZS0HaSD0XLjbaSjElhIr+AAAAAXRSTlMAQObYZgAAAHlJREFUCNdNyosOwyAIhWHAQS1Vt7a77/3fcxxdmv0xwmckutAR1nkm4ggbyEcg/wWmlGLDAA3oL50xi6fk5ffZ3E2E3QfZDCcCN2YtbEWZt+Drc6u6rlqv7Uk0LdKqqr5rk2UCRXOk0vmQKGfc94nOJyQjouF9H/wCc9gECEYfONoAAAAASUVORK5CYII=)](https://scala-steward.org)

# ZIO Apache Arrow

ZIO based wrapper for [Apache Arrow Java Implementation](https://arrow.apache.org/docs/java/index.html) that leverages
[ZIO Schema](https://zio.dev/zio-schema/) library to derive codecs
for [ValueVector](https://arrow.apache.org/docs/java/reference/index.html)
and [VectorSchemaRoot](https://arrow.apache.org/docs/java/reference/index.html).

## Why?

- **ZIO native** - utilizes various ZIO features to offer a FP-oriented way of working with the Arrow API.
- **Resource management** - guarantees an automatic [memory management](https://arrow.apache.org/docs/java/memory.html) by wrapping up operations on [BufferAllocator](https://arrow.apache.org/docs/java/memory.html#bufferallocator) in ZIO's [Scope](https://zio.dev/reference/resource/scope/).
- **ZIO Schema** - automatically derives codecs for Arrow's ValueVector and VectorSchemaRoot data types, enabling a seamless integration with the rest of the ZIO ecosystem.
- **Integration with Arrow ecosystem** - the [Core](#core) module provides the groundwork for integrating your application or library with various parts of the Arrow ecosystem such as [Datafusion](https://datafusion.apache.org), [Polars](https://pola.rs), [Apache Iceberg](https://iceberg.apache.org), and more.


## Content

- [Installation](#installation)
- [Modules](#modules)
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

## Modules

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
import me.mnedokushev.zio.apache.arrow.core.Allocator
import me.mnedokushev.zio.apache.arrow.core.codec._
import zio.schema._
import zio.schema.Factory._

case class MyData(a: Int, b: String)
object MyData {
  implicit val schema: Schema[MyData] = DeriveSchema.gen[MyData]
}

val intCodec            = ValueVectorCodec.intCodec
val listStringCodec     = ValueVectorCodec.listChunkCodec[String]
val structMyDataCodec   = ValueVectorCodec.structCodec[MyData]

ZIO.scoped(
  for {
    intVec             <- intCodec.encodeZIO(Chunk(1, 2, 3))
    intResult          <- intCodec.decodeZIO(vec) // [1, 2, 3]
    listStringVec      <- listStringCodec.encodeZIO(Chunk(Chunk("a", "b"), Chunk("c")))
    listStringResult   <- listStringCodec.decodeZIO(vec) // [["a", "b"], ["c"]]
    structMyDataVec    <- structMyDataCodec.encodeZIO(Chunk(MyData(1, "a"), MyData(2, "b")))
    structMyDataResult <- structMyDataCodec.decodeZIO(vec) // [{"a": 1, "b": a}, {"a": 2, "b": b}]
  } yield ()
).provide(Allocator.rootLayer())
```

###### Nullable

```scala
import me.mnedokushev.zio.apache.arrow.core.Allocator
import me.mnedokushev.zio.apache.arrow.core.codec._
import zio.schema._
import zio.schema.Factory._

val intCodec           = ValueVectorCodec.optionCodec[IntVector, Int]
val listStringCodec    = ValueVectorCodec.listChunkOptionCodec[String]
val optionListIntCodec = ValueVectorCodec.optionListChunkCodec[Int]

ZIO.scoped(
  for {
    intVec              <- intCodec.encodeZIO(Chunk(Some(1), None, Some(2)))
    intResult           <- intCodec.decodeZIO(vec) // [1, null, 2]
    listStringVec       <- listStringCodec.encodeZIO(Chunk(Chunk(Some("a"), None), Chunk(Some("b"))))
    listStringResult    <- listStringCodec.decodeZIO(vec) // [["a", null], ["b"]]
    optionListIntVec    <- optionListIntCodec.encodeZIO(Chunk(Some(Chunk("a", "b")), None, Some(Chunk("c"))))
    optionListIntResult <- optionListIntCodec.decodeZIO(vec) // [["a", "b"], null, ["c"]]
  } yield ()
).provide(Allocator.rootLayer())
```

##### Tabular

https://arrow.apache.org/docs/java/vector_schema_root.html
> The recommended usage is to create a single VectorSchemaRoot based on a known schema and populate data over and over into that root in a stream of batches, rather than creating a new instance each time.

The API is similar to the ValueVectorCodec. The main difference is that it is supposed to be used with user-defined case classes (aka 2D datasets) only.

###### SchemaEncoder

> describes the overall structure consisting of any number of columns. It holds a sequence of fields together with some optional schema-wide metadata (in addition to per-field metadata).

When working with tabular data, we need a way to convert ZIO Schema into [Arrow Schema](https://arrow.apache.org/docs/java/reference/org/apache/arrow/vector/types/pojo/Schema.html).

```scala
import me.mnedokushev.zio.apache.arrow.core.codec._
import zio.schema._

case class MyData(a: Int, b: String)
object MyData {
  implicit val schema: Schema[MyData] = 
    DeriveSchema.gen[MyData]
  implicit val schemaEncoder: SchemaEncoder[MyData] =
    SchemaEncoder.fromDefaultDeriver[MyData]
}

MyData.schemaEncoder.encode(MyData.schema) // Right(Schema<a: Int(64, true) not null, b: Utf8 not null>)
```

###### VectorSchemaRoot

```scala
import me.mnedokushev.zio.apache.arrow.core.Allocator
import me.mnedokushev.zio.apache.arrow.core.codec._
import me.mnedokushev.zio.apache.arrow.core.Tabular._
import zio.schema._
import zio.schema.Factory._

case class MyData(a: Int, b: String)
object MyData {
  implicit val schema: Schema[MyData] = 
    DeriveSchema.gen[MyData]
  implicit val schemaEncoder: SchemaEncoder[MyData] = 
    SchemaEncoder.fromDefaultDeriver[MyData]
}

val myDataCodec = VectorSchemaRootCodec.codec[MyData]

ZIO.scoped(
  for {
    root         <- Tabular.empty[MyData]
    myDataVec    <- myDataCodec.encodeZIO(Chunk(MyData(1, "a"), MyData(2, "b")))
    myDataResult <- myDataCodec.decodeZIO(myDataVec)
  } yield ()
).provide(Allocator.rootLayer())
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
import me.mnedokushev.zio.apache.arrow.core.Allocator
import me.mnedokushev.zio.apache.arrow.core.codec._
import me.mnedokushev.zio.apache.arrow.core.ipc._
import java.io.ByteArrayInputStream
import zio.schema._
import zio.schema.Factory._

case class MyData(a: Int, b: String)
object MyData {
  implicit val schema: Schema[MyData] = 
    DeriveSchema.gen[MyData]
  implicit val schemaEncoder: SchemaEncoder[MyData] = 
    SchemaEncoder.fromDefaultDeriver[MyData]
  implicit val encoder: VectorSchemaRootDecoder[MyData] =
    VectorSchemaRootDecoder.fromDefaultDeriver[MyData]
  implicit val decoder: VectorSchemaRootEncoder[MyData] =
    VectorSchemaRootEncoder.fromDefaultDeriver[MyData]
}

val payload = (1 to 8096).map(i => MyData(i, i.toString))

ZIO.scoped(
  for {
    out    <- writeStreaming[Any, MyData](ZStream.from(payload))
    result <- readStreaming[MyData](new ByteArrayInputStream(out.toByteArray)).runCollect // Chunk(MyData(1, "1"), ..., MyData(8096, "8096"))
  } yield ()
).provide(Allocator.rootLayer())
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
import me.mnedokushev.zio.apache.arrow.core.Allocator
import me.mnedokushev.zio.apache.arrow.core.codec._
import me.mnedokushev.zio.apache.arrow.datafusion._
import zio._
import zio.schema._

import java.nio.file.Paths

case class User(fname: String, lname: String, address: String, age: Long)
object User {
  implicit val schema: Schema[User]                                   =
    DeriveSchema.gen[User]
  implicit val schemaEncoder: SchemaEncoder[User]                     =
    Derive.derive[SchemaEncoder, User](SchemaEncoderDeriver.default)
  implicit val vectorSchemaRootDecoder: VectorSchemaRootDecoder[User] =
    VectorSchemaRootDecoder.fromDefaultDeriver[User]
}

(
  ZIO.serviceWithZIO[Context] { context =>
    for {
      _      <- context.registerCsv("test", Paths.get(getClass.getResource("/test.csv").toURI))
      df     <- context.sql("SELECT * FROM test WHERE fname = 'Dog'")
      result <- df.collect[User].runCollect // Chunk(User("Dog", "Cat", "NY", 3)))
    } yield ()
  }
).provide(Context.create, Allocator.rootLayer())
```

You can also write the data back to CSV or Parquet files using `df.writeCsv` and `df.writeParquet` methods.