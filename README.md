<img src="https://github.com/vitalii-honta/trembita/blob/master/media/trembita-p.png" alt="trembita"/>
 
## Description 
Blow out your data into Trembita and chose how to process them!


## Processing modules
- [core](./core) - lazy (parallel) collections
- [functional](./functional) - high-level [Cats](https://github.com/typelevel/cats) and [Shapeless](https://github.com/milessabin/shapeless) abstractions for easier aggregations
- [distributed](./distributed) - define an `ComputationCluster` and use it for your `Big Data`

## Data sources 
 - Any `Iterable` - just wrap your collection into `LazyCollection`
 - [cassandra connector](./cassandra_connector) - fetch rows from your `Cassandra` database with `CassandraSource`
 - [cassandra phantom](./cassandra_phantom) - provides [Phantom](https://github.com/outworkers/phantom) library support
 
## Miscelone
 - [trembita slf4j](./trembita_slf4j) - provides [slf4j](https://www.slf4j.org/) logging support. Use it with any compatible logging backend ([logback](https://logback.qos.ch/), [log4j](https://logging.apache.org/log4j/2.x/))
