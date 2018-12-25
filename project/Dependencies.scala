import sbt._

object Dependencies {
  val scalaV            = "2.12.8"
  val testV             = "3.0.4"
  val catsEffectsV      = "1.1.0"
  val shapelessV        = "2.3.3"
  val spireV            = "0.16.0"
  val cassandraDriverV  = "3.6.0"
  val phantomV          = "2.29.0"
  val slf4jV            = "1.7.25"
  val sparkV            = "2.4.0"
  val akkaV             = "2.5.19"
  val akkaHttpV         = "10.1.6"
  val alpakkaV          = "0.8"
  val catsConsoleV      = "0.5"
  val infinispanV       = "9.4.5.Final"
  val scalaJava8CompatV = "0.9.0"

  val commonDeps = Seq(
    "org.scalactic" %% "scalactic"   % testV withSources (),
    "org.scalatest" %% "scalatest"   % testV % "test" withSources (),
    "org.typelevel" %% "cats-effect" % catsEffectsV withSources (),
    "com.chuusai"   %% "shapeless"   % shapelessV withSources (),
    "org.typelevel" %% "spire"       % spireV withSources ()
  )

  object Cassandra {
    val driver       = "com.datastax.cassandra" % "cassandra-driver-core"   % cassandraDriverV withSources ()
    val driverExtras = "com.datastax.cassandra" % "cassandra-driver-extras" % cassandraDriverV withSources ()
  }

  val phantom = "com.outworkers"    %% "phantom-jdk8" % phantomV withSources ()
  val slf4j   = "org.slf4j"         % "slf4j-api"     % slf4jV withSources ()
  val console = "com.github.gvolpe" %% "console4cats" % catsConsoleV withSources ()

  object Macros {
    val resetallattrs = "org.scalamacros" %% "resetallattrs" % "1.0.0" withSources ()
  }

  object Spark {
    val core      = "org.apache.spark" %% "spark-core"      % sparkV withSources ()
    val sql       = "org.apache.spark" %% "spark-sql"       % sparkV withSources ()
    val streaming = "org.apache.spark" %% "spark-streaming" % sparkV withSources ()
  }

  object Akka {
    val actors  = "com.typesafe.akka"  %% "akka-actor"              % akkaV withSources ()
    val streams = "com.typesafe.akka"  %% "akka-stream"             % akkaV withSources ()
    val http    = "com.typesafe.akka"  %% "akka-http"               % akkaHttpV withSources ()
    val csv     = "com.lightbend.akka" %% "akka-stream-alpakka-csv" % alpakkaV withSources ()
  }

  object Infinispan {
    val core    = "org.infinispan" % "infinispan-core"    % infinispanV withSources ()
    val commons = "org.infinispan" % "infinispan-commons" % infinispanV withSources ()
  }

  object ScalaCompat {
    val java8compat = "org.scala-lang.modules" %% "scala-java8-compat" % scalaJava8CompatV withSources ()
  }
}
