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
  val akkaHttpV        = "10.1.6"
  val alpakkaV         = "0.8"
  val catsConsoleV     = "0.5"

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

  val phantom = "com.outworkers"    %% "phantom-jdk8" % phantomV
  val slf4j   = "org.slf4j"         % "slf4j-api"     % "1.7.25"
  val console = "com.github.gvolpe" %% "console4cats" % catsConsoleV

  object Macros {
    val resetallattrs = "org.scalamacros" %% "resetallattrs" % "1.0.0"
  }

  object Spark {
    val core      = "org.apache.spark" %% "spark-core"      % sparkV
    val sql       = "org.apache.spark" %% "spark-sql"       % sparkV
    val streaming = "org.apache.spark" %% "spark-streaming" % sparkV
  }

  object Akka {
    val actors  = "com.typesafe.akka"  %% "akka-actor"              % akkaV
    val streams = "com.typesafe.akka"  %% "akka-stream"             % akkaV
    val http    = "com.typesafe.akka"  %% "akka-http"               % akkaHttpV
    val csv     = "com.lightbend.akka" %% "akka-stream-alpakka-csv" % alpakkaV

  }
}
