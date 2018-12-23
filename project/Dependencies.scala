import sbt._

object Dependencies {
  val scalaV           = "2.12.8"
  val testV            = "3.0.4"
  val catsEffectsV     = "1.1.0"
  val shapelessV       = "2.3.3"
  val spireV           = "0.16.0"
  val cassandraDriverV = "3.6.0"
  val phantomV         = "2.29.0"
  val slf4jV           = "1.7.25"
  val sparkV           = "2.4.0"
  val akkaV            = "2.5.19"

  val commonDeps = Seq(
    "org.scalactic" %% "scalactic"   % testV,
    "org.scalatest" %% "scalatest"   % testV % "test",
    "org.typelevel" %% "cats-effect" % catsEffectsV,
    "com.chuusai"   %% "shapeless"   % shapelessV,
    "org.typelevel" %% "spire"       % spireV
  )

  object Cassandra {
    val driver       = "com.datastax.cassandra" % "cassandra-driver-core"   % cassandraDriverV
    val driverExtras = "com.datastax.cassandra" % "cassandra-driver-extras" % cassandraDriverV
  }

  val phantom = "com.outworkers" %% "phantom-jdk8" % phantomV
  val slf4j   = "org.slf4j"      % "slf4j-api"     % "1.7.25"

  object Macros {
    val resetallattrs = "org.scalamacros" %% "resetallattrs" % "1.0.0"
  }

  object Spark {
    val core = "org.apache.spark" %% "spark-core" % sparkV
    val sql  = "org.apache.spark" %% "spark-sql"  % sparkV
  }

  object Akka {
    val actors  = "com.typesafe.akka" %% "akka-actor"  % akkaV
    val streams = "com.typesafe.akka" %% "akka-stream" % akkaV
  }
}
