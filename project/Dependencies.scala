import sbt._

object Dependencies {
  val `scala-2-12`      = "2.12.8"
  val `scala-2-11`      = "2.11.12"
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
  val mockitoV          = "2.23.4"
  val sttpV             = "1.5.2"
  val log4jV            = "2.11.0"
  val log4jScalaV       = "11.0"

  object Testing {
    val scalastic = "org.scalactic" %% "scalactic"   % testV withSources ()
    val scalatest = "org.scalatest" %% "scalatest"   % testV withSources ()
    val mockito   = "org.mockito"   % "mockito-core" % mockitoV withSources ()
  }

  object Typelevel {
    val catsEffect = "org.typelevel" %% "cats-effect" % catsEffectsV withSources ()
    val shapeless  = "com.chuusai"   %% "shapeless"   % shapelessV withSources ()
    val spire      = "org.typelevel" %% "spire"       % spireV withSources ()
  }

  object Cassandra {
    val driver       = "com.datastax.cassandra" % "cassandra-driver-core"   % cassandraDriverV withSources ()
    val driverExtras = "com.datastax.cassandra" % "cassandra-driver-extras" % cassandraDriverV withSources ()
    val phantom      = "com.outworkers"         %% "phantom-jdk8"           % phantomV withSources ()
  }

  object Utils {
    val slf4j      = "org.slf4j"                % "slf4j-api"        % slf4jV withSources ()
    val log4j      = "org.apache.logging.log4j" % "log4j-api"        % log4jV withSources ()
    val log4jScala = "org.apache.logging.log4j" %% "log4j-api-scala" % log4jScalaV withSources ()
    val console    = "com.github.gvolpe"        %% "console4cats"    % catsConsoleV withSources ()
  }

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
    val testkit = "com.typesafe.akka"  %% "akka-testkit"            % akkaV % "test" withSources ()
  }

  object Infinispan {
    val core    = "org.infinispan" % "infinispan-core"          % infinispanV withSources ()
    val commons = "org.infinispan" % "infinispan-commons"       % infinispanV withSources ()
    val hotrod  = "org.infinispan" % "infinispan-client-hotrod" % infinispanV withSources ()
  }

  object ScalaCompat {
    val java8compat = "org.scala-lang.modules" %% "scala-java8-compat" % scalaJava8CompatV withSources ()
  }

  object Sttp {
    val core        = "com.softwaremill.sttp" %% "core"                           % sttpV
    val asyncHttp   = "com.softwaremill.sttp" %% "async-http-client-backend"      % sttpV
    val catsBackend = "com.softwaremill.sttp" %% "async-http-client-backend-cats" % sttpV
  }

  val commonDeps = Seq(
    Testing.scalastic,
    Testing.scalatest % "test",
    Typelevel.catsEffect,
    Typelevel.shapeless,
    Typelevel.spire
  )
}
