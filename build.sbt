import xerial.sbt.Sonatype._
import Dependencies._

lazy val scalaReflect = Def.setting {
  "org.scala-lang" % "scala-reflect" % scalaVersion.value
}

organization in ThisBuild := "ua.pp.itkpi"

val ScalaVersions = Seq(`scala-2.12`, `scala-2.13`)

def sonatypeProject(id: String, base: File) =
  Project(id, base)
    .enablePlugins(JmhPlugin)
    .settings(
      name := id,
      scalaVersion := `scala-2.13`,
      crossScalaVersions := ScalaVersions,
      publishTo := {
        val nexus = "https://oss.sonatype.org/"
        if (isSnapshot.value)
          Some("snapshots" at nexus + "content/repositories/snapshots")
        else
          Some("releases" at nexus + "service/local/staging/deploy/maven2")
      },
      updateOptions := updateOptions.value.withGigahorse(false),
      sourceDirectory in Jmh := (sourceDirectory in Test).value,
      classDirectory in Jmh := (classDirectory in Test).value,
      dependencyClasspath in Jmh := (dependencyClasspath in Test).value,
      compile in Jmh := (compile in Jmh).dependsOn(compile in Test).value,
      run in Jmh := (run in Jmh).dependsOn(Keys.compile in Jmh).evaluated,
      resolvers += Resolver.sonatypeRepo("releases")
    )
    .withCommonSettings()

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

lazy val logging_commons =
  sonatypeProject(id = "trembita-logging-commons", base = file("./utils/logging/commons"))
    .dependsOn(kernel)

lazy val slf4j =
  sonatypeProject(id = "trembita-slf4j", base = file("./utils/logging/slf4j"))
    .dependsOn(kernel, logging_commons)
    .settings(libraryDependencies ++= {
      Seq(Utils.slf4j)
    })

//lazy val trembita_spark =
//  sonatypeProject(id = "trembita-spark", base = file("./integrations/spark/core"))
////    .dependsOn(kernel)
//    .settings(
//      name := "trembita-spark",
//      scalacOptions ++= Seq(
//        "-language:experimental.macros"
//      ),
//      libraryDependencies ++= {
//        Seq(
//          Spark.core % "provided",
//          Spark.sql  % "provided",
//          Macros.resetallattrs
//        )
//      }
//    )
//    .scala212Only()

lazy val trembita_akka_streams =
  sonatypeProject(
    id = "trembita-akka-streams",
    base = file("./integrations/akka/streams")
  ).dependsOn(kernel)
    .settings(
      name := "trembita-akka-streams",
      libraryDependencies ++= {
        Seq(
          Akka.actors,
          Akka.streams,
          Akka.testkit
        )
      }
    )

//lazy val seamless_akka_spark =
//  sonatypeProject(
//    id = "trembita-seamless-akka-spark",
//    base = file("./integrations/seamless/akka-spark")
//  ) /*.dependsOn(kernel, trembita_akka_streams, trembita_spark)*/
//    .settings(
//      name := "trembita-seamless-akka-spark",
//      libraryDependencies ++= Seq(
//        Spark.core % "provided",
//        Spark.sql  % "provided"
//      )
//    )
//    .scala212Only()

//lazy val trembita_spark_streaming =
//  sonatypeProject(id = "trembita-spark-streaming", base = file("./integrations/spark/streaming"))
////    .dependsOn(kernel, trembita_spark)
//    .settings(
//      name := "trembita-spark-streaming",
//      scalacOptions ++= Seq(
//        "-language:experimental.macros"
//      ),
//      libraryDependencies ++= {
//        Seq(
//          Spark.core      % "provided",
//          Spark.sql       % "provided",
//          Spark.streaming % "provided"
//        )
//      }
//    )
//    .scala212Only()

lazy val trembita_caching =
  sonatypeProject(id = "trembita-caching", base = file("./caching/kernel"))
    .dependsOn(kernel)
    .settings(
      name := "trembita-caching"
    )

lazy val trembita_java_streams =
  sonatypeProject(id = "trembita-java-streams", base = file("./integrations/java/streams"))
    .dependsOn(kernel)
    .settings(
      name := "trembita_java_streams",
      scalacOptions ++= Seq(
        "-language:experimental.macros",
        "-target:jvm-1.8"
      ),
      libraryDependencies += ScalaCompat.java8compat
    )

//lazy val bench = Project(id = "trembita-bench", base = file("./bench"))
//  .enablePlugins(JmhPlugin)
//  .dependsOn(
//    collection_extentions,
//    kernel,
//    slf4j,
////    trembita_spark,
//    trembita_akka_streams,
////    seamless_akka_spark,
////    trembita_spark_streaming,
//    trembita_caching,
//    trembita_java_streams
//  )
//  .settings(
//    name := "trembita-bench",
//    skip in publish := true,
//    publish := {},
//    publishLocal := {},
////    libraryDependencies ++= {
////      Seq(
////        Spark.core,
////        Spark.sql,
////        Spark.streaming
////      ).map(_ exclude ("org.slf4j", "log4j-over-slf4j"))
////    }
//  )
//  .withCommonSettings()
//  .withMacroAnnotations()
////  .scala212Only()

lazy val examples = Project(id = "trembita-examples", base = file("./examples"))
  .dependsOn(
    collection_extentions,
    kernel,
    slf4j,
//    trembita_spark,
    trembita_akka_streams,
//    seamless_akka_spark,
//    trembita_spark_streaming,
    trembita_caching,
    trembita_java_streams
  )
  .settings(
    name := "trembita-examples",
    scalaVersion := `scala-2.13`,
    crossScalaVersions := ScalaVersions,
    skip in publish := true,
    publish := {},
    publishLocal := {},
    libraryDependencies ++= {
      Seq(
        Akka.csv,
        Akka.http,
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
  .withCommonSettings()
  .withMacroAnnotations()
  .withSpark()

lazy val root = Project(id = "trembita", base = file("."))
  .dependsOn(/*bench, */examples)
  .aggregate(
    collection_extentions,
    kernel,
    slf4j,
//    trembita_spark,
    trembita_akka_streams,
//    seamless_akka_spark,
//    trembita_spark_streaming,
    trembita_caching,
    logging_commons,
    trembita_java_streams
  )
  .settings(
    name := "trembita",
    scalaVersion := `scala-2.13`,
    crossScalaVersions := ScalaVersions,
    skip in publish := true,
    publish := {},
    publishLocal := {},
    coverageExcludedPackages := ".*operations.*",
    coverageExcludedFiles := ".*orderingInstances | .*arrows* | .*ToCaseClass* | .*22*"
  )
