[![codecov](https://codecov.io/gh/vitaliihonta/trembita/branch/master/graph/badge.svg)](https://codecov.io/gh/vitaliihonta/trembita)
[![Build Status](https://travis-ci.com/vitaliihonta/trembita.svg?branch=master)](https://travis-ci.com/vitaliihonta/trembita)

<img src="https://github.com/vitalii-honta/trembita/blob/master/media/trembita-p.png" alt="trembita"/>
 
## Description 
Project Trembita - Functional Data Pipelining library. 
Lets you query and transform your `not enough big` data in a pure functional, typesafe & declarative way.

```scala
resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
libraryDependencies ++= {
  val trembitaV = "0.2.0-SNAPSHOT"
  Seq(
    "com.github.vitaliihonta.trembita" %% "trembita-kernel" % trembitaV, // kernel,
    
    "com.github.vitaliihonta.trembita" %% "trembita-cassandra-connector" % trembitaV, // cassandra
    
    "com.github.vitaliihonta.trembita" %% "trembita-cassandra-connector-phantom" % trembitaV, // phantom
    
    "com.github.vitaliihonta.trembita" %% "trembita-slf4j" % trembitaV, // slf4j,
    
    "com.github.vitaliihonta.trembita" %% "trembita-circe" % trembitaV // circe
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
