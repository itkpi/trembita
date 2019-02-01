---
Project: Trembita
Current version: 0.8.5-SNAPSHOT
Scala version: 2.11.12, 2.12.8
---

[![codecov](https://codecov.io/gh/itkpi/trembita/branch/master/graph/badge.svg)](https://codecov.io/gh/itkpi/trembita)
[![Build Status](https://travis-ci.com/itkpi/trembita.svg?branch=master)](https://travis-ci.com/itkpi/trembita)
![Cats Friendly Badge](https://typelevel.org/cats/img/cats-badge-tiny.png) 

<img src="https://raw.githubusercontent.com/vitaliihonta/trembita/master/assets/trembita-p.png" alt="trembita"/>
 
## Description 
Project Trembita - Functional Data Pipelining library. 
Lets you query and transform your data in a pure functional, typesafe & declarative way.
Trembita allows you to make complicated transformation pipelines where some of them are executed locally sequentially, locally in parallel on in other environments (for instance on Spark cluster, see below)

```scala
resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
libraryDependencies ++= {
  val trembitaV = "0.8.5-SNAPSHOT"
  Seq(
    "ua.pp.itkpi" %% "trembita-kernel" % trembitaV, // kernel,
    
    "ua.pp.itkpi" %% "trembita-cassandra" % trembitaV, // cassandra
    
    "ua.pp.itkpi" %% "trembita-phantom" % trembitaV, // phantom
    
    "ua.pp.itkpi" %% "trembita-slf4j" % trembitaV // slf4j, for logging    
  )
}
```

## Core features

- [Typesafe and IDE friendly querying dsl](./examples/src/main/scala/com/examples/kernel/QLSample.scala) for data pipelines provides a unified model and typechecking for various data sources (including collections and Spark RDD)
- [Purely functional stateful transformations using Finite State Machines](./examples/src/main/scala/com/examples/kernel/FSMSample.scala) provides dsl for defining FSM that can be run on various data source (collections, Spark Datasets, Akka Streams...) 
- [One line caching](./caching)
- [Simple logging](./utils/logging)

## Available Integrations
- Apache Spark ([core](http://spark.apache.org/docs/latest/rdd-programming-guide.html), [SQL](http://spark.apache.org/docs/latest/sql-programming-guide.html))
- Apache Spark [Streaming](http://spark.apache.org/docs/latest/streaming-programming-guide.html)
- [Akka Streams](https://doc.akka.io/docs/akka/current/stream/)
- [Cassandra](http://cassandra.apache.org/)
- Cassandra using [phantom](https://github.com/outworkers/phantom))
- [Infinispan](http://infinispan.org/)
- [Java8 streams](https://www.oracle.com/technetwork/articles/java/ma14-java-se-8-streams-2177646.html)

## Processing modules
- [kernel](./kernel) - lazy (parallel) data pipelines, QL for grouping/aggregations and stateful computations using [Cats](https://github.com/typelevel/cats) and [Shapeless](https://github.com/milessabin/shapeless) 

## Data sources 
 - Any `Iterable` - just wrap your collection into `DataPipeline`
 - [cassandra connector](./cassandra_connector) - fetch rows from your `Cassandra` database with `CassandraSource`
 - [cassandra phantom](./cassandra_connector_phantom) - provides [Phantom](https://github.com/outworkers/phantom) library support
 - [akka stream](./integrations/akka/streams) - allows to make pipeline from akka stream (e.g. from any data source compatible with akka)
 - [spark RDD / DataSet](./integrations/spark/core) - allows to make pipeline from RDD / DataSet (e.g. from any non-streaming data source compatible with Spark)
 - [spark DStreams](./integrations/spark/streaming) - allows to make pipeline from Discrete streams (e.g. from any streaming data source compatible with Spark)
 
## Miscelone
 - [trembita slf4j](./utils/logging/slf4j) - provides [slf4j](https://www.slf4j.org/) logging support. Use it with any compatible logging backend (for instance [logback](https://logback.qos.ch/))
 - [trembita log4j](./utils/logging/log4j) - provides [log4j](https://logging.apache.org/log4j/2.x/manual/scala-api.html) logging support.
 
 ## Spark support
 ### Introducing spark pipelines 
You can run some your transformations on [spark](http://spark.apache.org/) cluster. 
To do that, add the following dependencies:
```scala
libraryDependencies ++= Seq(
    "ua.pp.itkpi" %% "trembita-spark" % trembitaV,
    "org.apache.spark" %% "spark-core" % "2.4.0" // first spark version with scala 2.12 support
)
```
### Asynchronous computations in spark
Using spark integration you can even easily run asynchronous computations on spark with Futures:
```scala
import trembita._
import trembita.spark._
import org.apache.spark._
import scala.concurrent.{ExecutionContext, Future}
import java.util.concurrent.Executors

implicit val sc: SparkContext = ??? // requires implicit SparkContext in scope
implicit val timeout: Timeout = Timeout(5.minutes) // requires implicit timeout for async operations
implicit val ec: ExecutionContext = ???

val cachedThreadPool =
    ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
    
Input
  .sequentialF[SerializableFuture, Seq]
  .create(SerializableFuture.pure(Seq(1, 2, 3, 20, 40, 60)))
  .to[Spark]
  // will be executed on spark
  .map(_ + 1)
  .mapM { i: Int =>
    val n = SerializableFuture.start { i + 1 }(cahedThreadPool)
    val b = SerializableFuture
      .start {
        val x = 1 + 2
        x * 3
      }
      .flatTap(
        xx =>
          SerializableFuture.start {
            println(s"spark debug: $xx") // you won't see this in submit logs
        }
      )

    val result: SerializableFuture[Int] =
      n.bind { nx =>
        b.where(nx > _).fmap(_ + nx)
      }

    result.attempt
  }
  .mapK(serializableFutureToIO)
  .map(_.getOrElse(-100500))
  .mapM { i =>
    IO { scala.util.Random.nextInt(10) + i }
  }
  // will be executed locally in parallel
  .to[Parallel]
  .info(i => s"parallel debug: $i") // you will see it in console
  .map(_ + 1)
```
Trembita will do the best to transform async lambda into serializable format.
By default a special macro detects all references to `ExecutionContext` within lambda you pass into `mapM`.
All `ExecutionContext`'s should be globally accessible (e.g. need to be `def` or `val` in some object).
If not - your code won't compile with appropriate error.
If everything is ok - macro creates helper object with references to all found `ExecutionContext`s making them `@transient lazy val` (well known technique) and rewrites your lambda so that all async transformations references to fields in that object.
You can find full example [here](./examples/src/main/scala/com/examples/spark/Main.scala).

Happy to say that using `cats.effect.IO` on spark is also supported =)
### FSM on Spark Datasets
You can now define stateful transformations on Spark Dataset using Finite State Machines.
It's implemented using `Dataset.mapWithState`.
Defining FSM for Spark is as simple as defining FSM for regular pipeline except of state is preserved only at level for specific `key` (due to `mapWithState` limitation).
To do so, use `fsmByKey`:
```scala
val pipeline: DataPipelineT[F, A, Spark] = ???
pipeline.fsmByKey(getKey = ???)(... /* your FSM definition here */)
```
Full example can be found [here](./examples/src/main/scala/com/examples/spark/FSMSample.scala).
### Typesafe QL on RDD
See the full example [here](./examples/src/main/scala/com/examples/spark/QLExample.scala)
### Limitations
 - Be careful not to make closures against the `SparkContext` or `SparkSession` because it will fall in runtime
 - Other non-serializable resources also will fail in runtime. This will be adapted later

### Examples
You can find a script to run the example on spark cluster within docker:
```bash
# in project root
sbt trembita-examples/assembly # prepare fat jar for spark-submit
sh examples/src/main/resources/spark/cluster/run.sh
```
To run Spark FSM example in docker use the following script:
```bash
# in project root
sbt trembita-examples/assembly # prepare fat jar for spark-submit
sh examples/src/main/resources/spark/cluster/run_fsm.sh
```

To run Spark QL example in docker use the following script:
```bash
# in project root
sbt trembita-examples/assembly # prepare fat jar for spark-submit
sh examples/src/main/resources/spark/cluster/run_ql.sh
```

Before running QL please remove [spire](https://github.com/non/spire) jars from spark classpath to avoid dependency conflicts

## Akka streams support
Trembita now supports running a part of your transformations on [akka-streams](https://doc.akka.io/docs/akka/current/stream/).
To use it, add the following dependency:
```scala
libraryDependencies += "ua.pp.itkpi" %% "trembita-akka-streams" % trembitaV
```

You can run existing pipeline through akka stream or create a pipeline from source directly:
```scala
import akka.stream.scaladsl._
import trembita.akka_streams._

val fileLines =
  Input.fromSourceF[IO, ByteString, Future[IOResult]](IO {
    FileIO
      .fromPath(Paths.get("examples/src/main/resources/words.txt"))
  })
```

Akka streaming pipelines also support `FSM` using custom graph state:
```scala
val pipeline: DataPipelineT[IO, Int, Akka] = ???
val stateful = pipeline.fsm(/* your FSM definition here */)
```
You can find full examples [here](./examples/src/main/scala/com/examples/akka)

## Seamless Akka to Spark integration
Add the following dependency if you wan't to run your pipeline through both akka streams and spark RDD:
```scala
libraryDependencies += "ua.pp.itkpi" %% "trembita-seamless-akka-spark" % trembitaV
```
It goal is to avoid additional overhead when switching between akka and spark.
`Akka -> Spark` is implemented using custom Sink.
`Spark -> Akka` is implemented using `toLocalIterator`

## Spark streaming support
Trembita now allows to write `QL` and `FSM` upon [spark DStreams](https://spark.apache.org/docs/latest/streaming-programming-guide.html).
```scala
libraryDependencies += "ua.pp.itkpi" %%  "trembita-spark-streaming" % trembitaV
```

For examples see [here](./examples/src/main/scala/com/examples/spark/streaming)
Run scripts:
- [basic](./examples/src/main/resources/spark/cluster/run_streaming.sh)
- [FSM](./examples/src/main/resources/spark/cluster/run_streaming_fsm.sh)
- [QL](./examples/src/main/resources/spark/cluster/run_streaming_ql.sh)

## java.util.stream integration
```scala
libraryDependencies += "ua.pp.itkpi" %%  "trembita-java-streams" % trembitaV
```

See [sources](integrations/java/streams) and [tests](integrations/java/streams/src/test/scala/trembita/jstreams) for examples

## To be done
- [x] caching
- [x] integration with distributed streaming frameworks
- [ ] tensorflow
- [ ] slick (in progress)
- [ ] akka http output

## Additional information
My speec about trembita at Scalaua conference:
https://youtu.be/PDBVCVv4mVc

## What means `trembita`?
<img src="http://typical.if.ua/wp-content/uploads/2015/12/213.jpg" alt="trembita"/>

Trembita is a alpine horn made of wood. It is common among Ukrainian highlanders Hutsuls who used to live in western Ukraine, eastern Poland, Slovakia and northern Romania. In southern Poland it's called trombita, bazuna in the North and ligawka in central Poland.

## Contributors

- [Vitalii Honta](https://github.com/vitaliihonta)
- You =)
