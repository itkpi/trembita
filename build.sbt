import xerial.sbt.Sonatype._
import Dependencies._

lazy val snapshot: Boolean = true
lazy val v: String = {
  val vv = "0.8.5"
  if (!snapshot) vv
  else vv + "-SNAPSHOT"
}

lazy val scalaReflect = Def.setting {
  "org.scala-lang" % "scala-reflect" % scalaVersion.value
}

organization in ThisBuild := "ua.pp.itkpi"

val ScalaVersions = Seq(`scala-2-12`, `scala-2-11`)

def sonatypeProject(id: String, base: File) =
  Project(id, base)
    .enablePlugins(JmhPlugin)
    .settings(
      name := id,
      isSnapshot := snapshot,
      version := v,
      scalaVersion := `scala-2-12`,
      crossScalaVersions := ScalaVersions,
      publishTo := {
        val nexus = "https://oss.sonatype.org/"
        if (isSnapshot.value)
          Some("snapshots" at nexus + "content/repositories/snapshots")
        else
          Some("releases" at nexus + "service/local/staging/deploy/maven2")
      },
      updateOptions := updateOptions.value.withGigahorse(false),
      scalacOptions ++= Seq("-Ypartial-unification", "-feature"),
      sourceDirectory in Jmh := (sourceDirectory in Test).value,
      classDirectory in Jmh := (classDirectory in Test).value,
      dependencyClasspath in Jmh := (dependencyClasspath in Test).value,
      compile in Jmh := (compile in Jmh).dependsOn(compile in Test).value,
      run in Jmh := (run in Jmh).dependsOn(Keys.compile in Jmh).evaluated,
      resolvers += Resolver.sonatypeRepo("releases"),
      addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.9.8"),
      libraryDependencies ++= commonDeps
    )

lazy val collection_extentions = sonatypeProject(
  id = "collection-extensions",
  base = file("./utils/collection_extensions")
)

lazy val kernel =
  sonatypeProject(id = "trembita-kernel", base = file("./kernel"))
    .dependsOn(collection_extentions)
    .settings(libraryDependencies ++= {
      Seq("org.scalatest" %% "scalatest" % testV % "test")
    })

lazy val cassandra_connector = sonatypeProject(
  id = "trembita-cassandra",
  base = file("./connectors/cassandra")
).dependsOn(kernel)
  .settings(libraryDependencies ++= {
    Seq(
      Cassandra.driver
    )
  })

lazy val cassandra_connector_phantom =
  sonatypeProject(id = "trembita-phantom", base = file("./connectors/phantom"))
    .dependsOn(cassandra_connector)
    .settings(libraryDependencies ++= {
      Seq(
        Cassandra.phantom      % "provided",
        Cassandra.driverExtras % "provided"
      )
    })

lazy val logging_commons =
  sonatypeProject(id = "trembita-logging-commons", base = file("./utils/logging/commons"))
    .dependsOn(kernel)

lazy val slf4j =
  sonatypeProject(id = "trembita-slf4j", base = file("./utils/logging/slf4j"))
    .dependsOn(kernel, logging_commons)
    .settings(libraryDependencies ++= {
      Seq(Utils.slf4j)
    })

lazy val log4j =
  sonatypeProject(id = "trembita-log4j", base = file("./utils/logging/log4j"))
    .dependsOn(kernel, logging_commons)
    .settings(libraryDependencies ++= {
      Seq(
        Utils.log4j,
        Utils.log4jScala
      )
    })

lazy val trembita_spark =
  sonatypeProject(id = "trembita-spark", base = file("./integrations/spark/core"))
    .dependsOn(kernel)
    .settings(
      name := "trembita-spark",
      version := v,
      scalacOptions ++= Seq(
        "-Ypartial-unification",
        "-language:experimental.macros"
      ),
      libraryDependencies ++= {
        Seq(
          Spark.core % "provided",
          Spark.sql  % "provided",
          Macros.resetallattrs
        )
      }
    )

lazy val trembita_akka_streams =
  sonatypeProject(
    id = "trembita-akka-streams",
    base = file("./integrations/akka/streams")
  ).dependsOn(kernel)
    .settings(
      name := "trembita-akka-streams",
      version := v,
      scalacOptions ++= Seq("-Ypartial-unification"),
      libraryDependencies ++= {
        Seq(
          Akka.actors,
          Akka.streams,
          Akka.testkit
        )
      }
    )

lazy val seamless_akka_spark =
  sonatypeProject(
    id = "trembita-seamless-akka-spark",
    base = file("./integrations/seamless/akka-spark")
  ).dependsOn(kernel, trembita_akka_streams, trembita_spark)
    .settings(
      name := "trembita-seamless-akka-spark",
      version := v,
      scalacOptions ++= Seq("-Ypartial-unification"),
      libraryDependencies ++= Seq(
        Spark.core % "provided",
        Spark.sql  % "provided"
      ),
      addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.9.8")
    )

lazy val trembita_spark_streaming =
  sonatypeProject(id = "trembita-spark-streaming", base = file("./integrations/spark/streaming"))
    .dependsOn(kernel, trembita_spark)
    .settings(
      name := "trembita-spark-streaming",
      version := v,
      scalacOptions ++= Seq(
        "-Ypartial-unification",
        "-language:experimental.macros"
      ),
      libraryDependencies ++= {
        Seq(
          Spark.core      % "provided",
          Spark.sql       % "provided",
          Spark.streaming % "provided"
        )
      }
    )

lazy val trembita_caching =
  sonatypeProject(id = "trembita-caching", base = file("./caching/kernel"))
    .dependsOn(kernel)
    .settings(
      name := "trembita-caching",
      version := v
    )

lazy val trembita_caching_infinispan =
  sonatypeProject(id = "trembita-caching-infinispan", base = file("./caching/infinispan"))
    .dependsOn(trembita_caching)
    .settings(
      name := "trembita-caching-infinispan",
      version := v,
      libraryDependencies ++= Seq(
        Infinispan.core,
        Infinispan.commons,
        ScalaCompat.java8compat,
        Testing.mockito % "test"
      )
    )

lazy val trembita_java_streams =
  sonatypeProject(id = "trembita-java-streams", base = file("./integrations/java/streams"))
    .dependsOn(kernel)
    .settings(
      name := "trembita_java_streams",
      version := v,
      scalacOptions ++= Seq(
        "-Ypartial-unification",
        "-language:experimental.macros",
        "-target:jvm-1.8"
      ),
      libraryDependencies += ScalaCompat.java8compat
    )

lazy val seamless_akka_infinispan =
  sonatypeProject(id = "trembita-seamless-akka-infinispan", base = file("./integrations/seamless/akka-infinispan"))
    .dependsOn(kernel, trembita_akka_streams, trembita_caching_infinispan)
    .settings(
      name := "trembita-seamless-akka-infinispan",
      version := v,
      scalacOptions ++= Seq(
        "-Ypartial-unification",
        "-language:experimental.macros",
        "-target:jvm-1.8"
      ),
      libraryDependencies ++= Seq(
        ScalaCompat.java8compat,
        Akka.testkit,
        Testing.mockito % "test"
      )
    )

lazy val trembita_zio =
  sonatypeProject(id = "trembita-zio", base = file("./integrations/zio"))
    .dependsOn(kernel)
    .settings(
      name := "trembita-zio",
      version := v,
      scalacOptions ++= Seq(
        "-Ypartial-unification",
        "-language:experimental.macros",
        "-target:jvm-1.8"
      ),
      libraryDependencies ++= Seq(
        Scalaz.zio,
        Scalaz.interopCats,
        Testing.mockito % "test"
      )
    )

lazy val bench = Project(id = "trembita-bench", base = file("./bench"))
  .enablePlugins(JmhPlugin)
  .dependsOn(
    collection_extentions,
    kernel,
    slf4j,
    cassandra_connector,
    cassandra_connector_phantom,
    trembita_spark,
    trembita_akka_streams,
    seamless_akka_spark,
    trembita_spark_streaming,
    trembita_caching,
    trembita_caching_infinispan,
    trembita_java_streams,
    seamless_akka_infinispan,
    trembita_zio
  )
  .settings(
    name := "trembita-bench",
    version := v,
    scalacOptions += "-Ypartial-unification",
    scalaVersion := `scala-2-12`,
    crossScalaVersions := ScalaVersions,
    isSnapshot := snapshot,
    skip in publish := true,
    publish := {},
    publishLocal := {},
    addCompilerPlugin(
      "org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.full
    ),
    addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.9.8"),
    libraryDependencies ++= {
      Seq(
        Spark.core,
        Spark.sql,
        Spark.streaming
      ).map(_ exclude ("org.slf4j", "log4j-over-slf4j"))
    }
  )

lazy val examples = Project(id = "trembita-examples", base = file("./examples"))
  .dependsOn(
    collection_extentions,
    kernel,
    slf4j,
    cassandra_connector,
    cassandra_connector_phantom,
    trembita_spark,
    trembita_akka_streams,
    seamless_akka_spark,
    trembita_spark_streaming,
    trembita_caching,
    trembita_caching_infinispan,
    trembita_java_streams,
    seamless_akka_infinispan,
    trembita_zio
  )
  .settings(
    name := "trembita-examples",
    version := v,
    scalacOptions += "-Ypartial-unification",
    scalaVersion := `scala-2-12`,
    crossScalaVersions := ScalaVersions,
    isSnapshot := snapshot,
    skip in publish := true,
    publish := {},
    publishLocal := {},
    addCompilerPlugin(
      "org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.full
    ),
    addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.9.8"),
    libraryDependencies ++= {
      Seq(
        Cassandra.driver,
        Cassandra.driverExtras,
        Cassandra.phantom,
        Spark.core      % "provided",
        Spark.sql       % "provided",
        Spark.streaming % "provided",
        Utils.console,
        Akka.csv,
        Akka.http,
        Infinispan.hotrod,
        Sttp.core,
        Sttp.catsBackend,
        Sttp.asyncHttp
      ).map(_ exclude ("org.slf4j", "log4j-over-slf4j"))
    },
    test in assembly := {},
    mainClass in assembly := Some("com.examples.spark.Main"),
    assemblyJarName in assembly := "trembita-spark.jar",
    assemblyMergeStrategy in assembly := {
      case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
      case m if m.toLowerCase.matches("meta-inf.*\\.sf$") =>
        MergeStrategy.discard
      case "log4j.properties" => MergeStrategy.discard
      case m if m.toLowerCase.startsWith("meta-inf/services/") =>
        MergeStrategy.filterDistinctLines
      case "reference.conf"        => MergeStrategy.concat
      case m if m endsWith ".conf" => MergeStrategy.concat
      case _                       => MergeStrategy.first
    }
  )

lazy val root = Project(id = "trembita", base = file("."))
  .dependsOn(bench, examples)
  .aggregate(
    collection_extentions,
    kernel,
    slf4j,
    cassandra_connector,
    cassandra_connector_phantom,
    trembita_spark,
    trembita_akka_streams,
    seamless_akka_spark,
    trembita_spark_streaming,
    trembita_caching,
    trembita_caching_infinispan,
    log4j,
    logging_commons,
    trembita_java_streams,
    seamless_akka_infinispan,
    trembita_zio
  )
  .settings(
    name := "trembita",
    version := v,
    scalaVersion := `scala-2-12`,
    crossScalaVersions := ScalaVersions,
    scalacOptions += "-Ypartial-unification",
    isSnapshot := snapshot,
    skip in publish := true,
    publish := {},
    publishLocal := {},
    coverageExcludedPackages := ".*operations.*",
    coverageExcludedFiles := ".*orderingInstances | .*arrows* | .*ToCaseClass* | .*22*"
  )
