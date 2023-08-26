# Codecs

```scala
val codec = ValueVectorCodec[Int, IntVector]

ZIO.scoped(
  for {
    vec <- codec.encodeZIO(Chunk(1, 2, 3))
    result <- codec.decodeZIO(vec)
  } yield result
)
```