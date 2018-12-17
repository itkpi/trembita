[![codecov](https://codecov.io/gh/vitaliihonta/trembita/branch/master/graph/badge.svg)](https://codecov.io/gh/vitaliihonta/trembita)
[![Build Status](https://travis-ci.com/vitaliihonta/trembita.svg?branch=master)](https://travis-ci.com/vitaliihonta/trembita)

<img src="https://github.com/vitalii-honta/trembita/blob/master/media/trembita-p.png" alt="trembita"/>
 
## Description 
Project Trembita - Functional Data Pipelining library. 
Lets you query and transform your data in a pure functional, typesafe & declarative way.
Trembita allows you to make complecated transformation pipelines where some of them are executed locally sequentially, locally in parallel on in other enviroments (for instance on Spark cluster, see belove)

```scala
resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
libraryDependencies ++= {
  val trembitaV = "0.3.0-SNAPSHOT"
  Seq(
    "com.github.vitaliihonta.trembita" %% "trembita-kernel" % trembitaV, // kernel,
    
    "com.github.vitaliihonta.trembita" %% "trembita-cassandra-connector" % trembitaV, // cassandra
    
    "com.github.vitaliihonta.trembita" %% "trembita-cassandra-connector-phantom" % trembitaV, // phantom
    
    "com.github.vitaliihonta.trembita" %% "trembita-slf4j" % trembitaV, // slf4j, for logging
    
    "com.github.vitaliihonta.trembita" %% "trembita-circe" % trembitaV // circe, for transforming query results into json
  )
}
```


## Processing modules
- [kernel](./kernel) - lazy (parallel) data pipelines, QL for grouping/aggregations and stateful computations using [Cats](https://github.com/typelevel/cats) and [Shapeless](https://github.com/milessabin/shapeless) 

## Data sources 
 - Any `Iterable` - just wrap your collection into `DataPipeline`
 - [cassandra connector](./cassandra_connector) - fetch rows from your `Cassandra` database with `CassandraSource`
 - [cassandra phantom](./cassandra_connector_phantom) - provides [Phantom](https://github.com/outworkers/phantom) library support
 
## Miscelone
 - [trembita slf4j](./trembita-slf4j) - provides [slf4j](https://www.slf4j.org/) logging support. Use it with any compatible logging backend ([logback](https://logback.qos.ch/), [log4j](https://logging.apache.org/log4j/2.x/))
 - [trembita circe](./serialization/circe) - allows to convert aggregation results directly into JSON using [Circe](https://github.com/circe/circe)
 
 ## Experimental: Spark support
 ### Introducing spark pipelines 
You can run some your transformations on [spark](http://spark.apache.org/) cluster. 
To do that, add the following dependencies:
```scala
libraryDependencies ++= Seq(
    "com.github.vitaliihonta.trembita" %% "trembita-spark" % trembitaV,
    "org.apache.spark" %% "spark-core" % "2.4.0" // first spark version with scala 2.12 support
)
```
### Asynchronous computations in spark
Using spark integration you can even easily run asynchronous computations on spark with Futures:
```scala
import com.github.trembita._
import com.github.trembita.experimental.spark._
import org.apache.spark._
import scala.concurrent.{ExecutionContext, Future}
import java.util.concurrent.Executors

implicit val sc: SparkContext = ??? // requires implicit SparkContext in scope
implicit val timeout: Timeout = Timeout(5.minutes) // requires implicit timeout for async operations
implicit val ec: ExecutionContext = ???

val cahedThreadPool =
    ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
    
val numbers = DataPipelineT[Future, Int](1, 2, 3, 20, 40, 60) // some basic pipeline
  // will be executed on spark
numbers
  .to[Spark]
   // belove transformations will be executed on spark
  .map(_ + 1)
  .mapM { i: Int =>
    val n = Future { i + 1 }(cahedThreadPool)
    val b = Future {
      val x = 1 + 2
      x * 3
    }

    for {
      nx <- n
      bx <- b
    } yield nx + bx)
  }
  .eval // collects results into driver program
```
Trembita will do the best to transform async lambda into serializable format.
By default a special macro detects all references to `ExecutionContext` within lambda you pass into `mapM`.
All `ExecutionContext`'s should be globally accessable (e.g. need to be `def` or `val` in some object).
If not - your code won't compile with appropriate error.
If everyting is ok - macro creates helper object with references to all found `ExecutionContext`s making them `@transient lazy val` (well known technique) and rewrites your lambda so that all async transformations references to fields in that object.
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
### Limitations
 - Be careful not to make closures against the `SparkContext` because it will fall in runtime
 - Other non-serializable resources also will fail in runtime. This will be adapted later
 - QL for spark is in progress. It would be a type-safe wrapper for native [Spark SQL](http://spark.apache.org/docs/latest/sql-programming-guide.html)

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

## Experimental: Akka streams support
Trembita now supports running a part of your transformations on [akka-streams](https://doc.akka.io/docs/akka/current/stream/).
To use it, add the following dependency:
```scala
libraryDependencies += "com.github.vitaliihonta.trembita" %% "trembita-akka-streams" % trembitaV
```

You can run existing pipeline throught akka stream or create a pipeline from source directly:
```scala
import akka.stream.scaladsl._
import com.github.trembita.experimental.akka._

val fileLines =
  DataPipelineT
    .fromReprF[IO, ByteString, Akka](IO {
      FileIO
        .fromPath(Paths.get(getClass.getResource("/words.txt").toURI))
        .mapMaterializedValue(_ => NotUsed)
    })
```

Akka streaming pipelines also support `FSM` using custom graph state:
```scala
val pipeline: DataPipelineT[IO, Int, Akka] = ???
val stateful = pipeline.fsm(/* your FSM definition here */)
```
You can find full examples [here](./examples/src/main/scala/com/examples/akka)
