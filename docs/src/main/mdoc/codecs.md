# Codecs

```scala
val codec = ValueVectorCodec[Int, IntVector]

ZIO.scoped(
  for {
    vec <- codec.encodeZIO(Chunk.empty)
    result <- codec.decodeZIO(vec)
  } yield result
)
```