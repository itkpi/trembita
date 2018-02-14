lazy val core = Project(id = "core", base = file("./core"))
  .settings(
    name := "core",
    version := "0.1",
    scalaVersion := "2.12.4",
    libraryDependencies ++= {
      val testV = "3.0.4"
      Seq(
        "org.scalactic" %% "scalactic" % testV,
        "org.scalatest" %% "scalatest" % testV % "test"
      )
    }
  )

lazy val cassandra_connector = Project(id = "cassandra_connector", base = file("./cassandra_connector"))
  .dependsOn(core)
  .settings(
    name := "cassandra_connector",
    version := "0.1",
    scalaVersion := "2.12.4",
    libraryDependencies ++= {
      Seq(
        "com.datastax.cassandra" % "cassandra-driver-core" % "3.4.0"
      )
    }
  )

lazy val cassandra_connector_phantom = Project(id = "cassandra_connector_phantom", base = file("./cassandra_connector_phantom"))
  .dependsOn(cassandra_connector)
  .settings(
    name := "cassandra_connector_phantom",
    version := "0.1",
    scalaVersion := "2.12.4",
    libraryDependencies ++= {
      Seq(
        "com.outworkers" %% "phantom-jdk8" % "2.20.0",
        "com.datastax.cassandra" % "cassandra-driver-extras" % "3.4.0"
      )
    }
  )

lazy val functional = Project(id = "functional", base = file("./functional"))
  .dependsOn(core)
  .settings(
    name := "functional",
    version := "0.1",
    scalaVersion := "2.12.4",
    scalacOptions += "-Ypartial-unification",
    libraryDependencies ++= {
      Seq(
        "org.typelevel" %% "cats-core" % "1.0.1",
        "com.chuusai" %% "shapeless" % "2.3.3"
      )
    }
  )

lazy val slf4j = Project(id = "trembita-slf4j", base = file("./trembita-slf4j"))
  .dependsOn(core)
  .settings(
    name := "trembita-slf4j",
    version := "0.1",
    scalaVersion := "2.12.4",
    libraryDependencies ++= {
      Seq(
        "org.slf4j" % "slf4j-api" % "1.7.25"
      )
    }
  )

lazy val distributed_internal = Project(
  id = "distributed_internal",
  base = file("./distributed_internal"))
  .dependsOn(core, slf4j)
  .settings(
    name := "distributed-internal",
    version := "0.1",
    scalaVersion := "2.12.4",
    libraryDependencies ++= {
      Seq(
        "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
        "ch.qos.logback" % "logback-classic" % "1.1.7",
        "com.typesafe.akka" %% "akka-cluster" % "2.5.5",
        "com.github.romix.akka" %% "akka-kryo-serialization" % "0.5.1"
      )
    }
  )

lazy val distributed_worker = Project(
  id = "distributed_worker",
  base = file("./distributed_worker"))
  .dependsOn(distributed_internal)
  .settings(
    name := "distributed_worker",
    version := "0.1",
    scalaVersion := "2.12.4",
    mainClass in Compile := Some("com.github.vitaliihonta.trembita.distributed.bootstrap.WorkerMain")
  )

lazy val distributed = Project(
  id = "distributed",
  base = file("./distributed"))
  .dependsOn(distributed_internal)
  .settings(
    name := "distributed",
    version := "0.1",
    scalaVersion := "2.12.4"
  )

lazy val root = Project(id = "trembita", base = file("."))
  .aggregate(
    core, functional, slf4j,
    cassandra_connector_phantom,
    distributed, distributed_worker
  )
  .settings(
    name := "trembita",
    version := "0.1",
    scalaVersion := "2.12.4"
  )
