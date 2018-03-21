import xerial.sbt.Sonatype._


lazy val snapshot: Boolean = true
lazy val v: String = {
  val vv = "0.1.2"
  if (!snapshot) vv
  else vv + "-SNAPSHOT"
}

lazy val scalaReflect = Def.setting {
  "org.scala-lang" % "scala-reflect" % scalaVersion.value
}

organization in ThisBuild := "com.datarootlabs.trembita"

def sonatypeProject(id: String, base: File) = Project(id, base)
  .enablePlugins(JmhPlugin)
  .settings(
    name := id,
    isSnapshot := snapshot,
    version := v,
    scalaVersion := "2.12.4",
    publishTo := {
      val nexus = "https://oss.sonatype.org/"
      if (isSnapshot.value)
        Some("snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("releases" at nexus + "service/local/staging/deploy/maven2")
    },
    scalacOptions += "-Ypartial-unification",
    sourceDirectory in Jmh := (sourceDirectory in Test).value,
    classDirectory in Jmh := (classDirectory in Test).value,
    dependencyClasspath in Jmh := (dependencyClasspath in Test).value,
    compile in Jmh := (compile in Jmh).dependsOn(compile in Test).value,
    run in Jmh := (run in Jmh).dependsOn(Keys.compile in Jmh).evaluated
  )

lazy val core = sonatypeProject(id = "trembita-core", base = file("./core"))
  .settings(
    libraryDependencies ++= {
      val testV = "3.0.4"
      Seq(
        "org.scalactic" %% "scalactic" % testV,
        "org.scalatest" %% "scalatest" % testV % "test"
      )
    }
  )

lazy val cassandra_connector = sonatypeProject(id = "trembita-cassandra_connector", base = file("./cassandra_connector"))
  .dependsOn(core)
  .settings(
    libraryDependencies ++= {
      Seq(
        "com.datastax.cassandra" % "cassandra-driver-core" % "3.4.0"
      )
    }
  )

lazy val cassandra_connector_phantom = sonatypeProject(id = "trembita-cassandra_connector_phantom", base = file("./cassandra_connector_phantom"))
  .dependsOn(cassandra_connector)
  .settings(
    libraryDependencies ++= {
      Seq(
        "com.outworkers" %% "phantom-jdk8" % "2.20.0",
        "com.datastax.cassandra" % "cassandra-driver-extras" % "3.4.0"
      )
    }
  )

lazy val trembitaql = sonatypeProject(id = "trembitaql", base = file("./trembitaql"))
  .dependsOn(core)
  .settings(
    name := "trembitaql",
    isSnapshot := snapshot,
    version := v,
    scalaVersion := "2.12.4",
    scalacOptions += "-Ypartial-unification",
    libraryDependencies ++= {
      Seq(
        "org.typelevel" %% "cats-core" % "1.0.1",
        "com.chuusai" %% "shapeless" % "2.3.3",
        scalaReflect.value
      )
    }
  )

lazy val slf4j = sonatypeProject(id = "trembita-slf4j", base = file("./trembita-slf4j"))
  .dependsOn(core)
  .settings(
    libraryDependencies ++= {
      Seq(
        "org.slf4j" % "slf4j-api" % "1.7.25"
      )
    }
  )

lazy val distributed_internal = sonatypeProject(
  id = "trembita-distributed_internal",
  base = file("./distributed_internal"))
  .dependsOn(core, slf4j)
  .settings(
    libraryDependencies ++= {
      Seq(
        "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
        "ch.qos.logback" % "logback-classic" % "1.1.7",
        "com.typesafe.akka" %% "akka-cluster" % "2.5.5",
        "com.github.romix.akka" %% "akka-kryo-serialization" % "0.5.1"
      )
    }
  )

lazy val distributed_worker = sonatypeProject(
  id = "trembita-distributed_worker",
  base = file("./distributed_worker"))
  .dependsOn(distributed_internal)
  .enablePlugins(DockerPlugin)
  .settings(
    mainClass in Compile := Some("com.datarootlabs.trembita.distributed.bootstrap.WorkerMain"),
    assemblyJarName in assembly := "trembita-distributed_worker.jar",
    dockerfile in docker := {
      // The assembly task generates a fat JAR file
      val artifact: File = assembly.value
      val artifactTargetPath = s"/app/${artifact.name}"

      new Dockerfile {
        from("openjdk:8-jre-alpine")
        maintainer("Vitalii Honta, Scala DevOps at Dataroot Labs")
        add(artifact, artifactTargetPath)
        env("CLUSTER_MASTER_HOST", "127.0.0.1")
        env("CLUSTER_MASTER_PORT", "2551")
        env("WORKER_HOST", "127.0.0.1")
        env("WORKER_PORT", "2551")
        env("CLUSTER_TOKEN", "your-token")
        entryPoint("java", "-jar", artifactTargetPath)
      }
    },
    imageNames in docker := Seq("latest").map { imgTag =>
      ImageName(
        repository = "trembita-distributed_worker",
        tag = Some(imgTag)
      )
    }
  )

lazy val distributed = sonatypeProject(
  id = "trembita-distributed",
  base = file("./distributed"))
  .dependsOn(distributed_internal)

lazy val trembitason = sonatypeProject(id = "trembitason", base = file("./trembitason"))
  .dependsOn(trembitaql)
  .settings(
    name := "trembitason",
    version := v,
    scalaVersion := "2.12.4",
    scalacOptions += "-Ypartial-unification",
    libraryDependencies ++= {
      val circeV = "0.9.0"
      Seq(
        "io.circe" %% "circe-core" % circeV,
        "io.circe" %% "circe-generic" % circeV,
        "io.circe" %% "circe-parser" % circeV
      )
    }
  )

lazy val examples = Project(id = "trembita-examples", base = file("./examples"))
  .dependsOn(
    core, slf4j,
    trembitaql, trembitason,
    cassandra_connector,
    cassandra_connector_phantom,
    distributed_internal,
    distributed, distributed_worker
  )
  .settings(
    name := "trembita-examples",
    version := v,
    scalaVersion := "2.12.4",
    scalacOptions += "-Ypartial-unification",
    isSnapshot := snapshot,
    skip in publish := true,
    publish := {},
    publishLocal := {}
  )

lazy val root = Project(id = "trembita", base = file("."))
  .aggregate(
    core, trembitaql, slf4j,
    cassandra_connector,
    cassandra_connector_phantom,
    distributed_internal,
    distributed, distributed_worker
  )
  .settings(
    name := "trembita",
    version := v,
    scalaVersion := "2.12.4",
    scalacOptions += "-Ypartial-unification",
    isSnapshot := snapshot,
    skip in publish := true,
    publish := {},
    publishLocal := {}
  )
