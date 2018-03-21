<img src="https://github.com/vitalii-honta/trembita/blob/master/media/trembita-p.png" alt="trembita"/>
 
## Description 
Project Trembita - Functional Data Pipelining library.


## Processing modules
- [core](./core) - lazy (parallel) collections
- [trembita QL](./trembitaql) - high-level abstractions for easier aggregations using [Cats](https://github.com/typelevel/cats) and [Shapeless](https://github.com/milessabin/shapeless) 
- [distributed](./distributed) - define an `ComputationCluster` and use it for your `Big Data` (NOT READY YET)

## Data sources 
 - Any `Iterable` - just wrap your collection into `LazyCollection`
 - [cassandra connector](./cassandra_connector) - fetch rows from your `Cassandra` database with `CassandraSource`
 - [cassandra phantom](./cassandra_connector_phantom) - provides [Phantom](https://github.com/outworkers/phantom) library support
 
## Miscelone
 - [trembita slf4j](./trembita-slf4j) - provides [slf4j](https://www.slf4j.org/) logging support. Use it with any compatible logging backend ([logback](https://logback.qos.ch/), [log4j](https://logging.apache.org/log4j/2.x/))
 - [trembitaSON](./trembitason) - converters from `ArbitraryGroupResult` to JSON using [Circe](https://github.com/circe/circe)