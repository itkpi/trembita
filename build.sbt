import xerial.sbt.Sonatype._

lazy val snapshot: Boolean = true
lazy val v: String = {
  val vv = "0.2.0"
  if (!snapshot) vv
  else vv + "-SNAPSHOT"
}

lazy val scalaReflect = Def.setting {
  "org.scala-lang" % "scala-reflect" % scalaVersion.value
}

organization in ThisBuild := "com.github.vitaliihonta.trembita"

val scalaV = "2.12.8"
val testV = "3.0.4"
val catsEffectsV = "1.1.0"
val shapelessV = "2.3.3"
val spireV = "0.16.0"

val commonDeps = Seq(
  "org.scalactic" %% "scalactic" % testV,
  "org.scalatest" %% "scalatest" % testV % "test",
  "org.typelevel" %% "cats-effect" % catsEffectsV,
  "com.chuusai" %% "shapeless" % shapelessV,
  "org.typelevel" %% "spire" % spireV
)

def sonatypeProject(id: String, base: File) =
  Project(id, base)
    .enablePlugins(JmhPlugin)
    .settings(
      name := id,
      isSnapshot := snapshot,
      version := v,
      scalaVersion := scalaV,
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
      run in Jmh := (run in Jmh).dependsOn(Keys.compile in Jmh).evaluated,
      resolvers += Resolver.sonatypeRepo("releases"),
      addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.9.8"),
      libraryDependencies ++= commonDeps
    )

lazy val collection_extentions = sonatypeProject(
  id = "collection-extentions",
  base = file("./collection_extentions")
)

lazy val kernel =
  sonatypeProject(id = "trembita-kernel", base = file("./kernel"))
    .dependsOn(collection_extentions)
    .settings(libraryDependencies ++= {
      Seq("org.scalatest" %% "scalatest" % testV % "test")
    })

lazy val cassandra_connector = sonatypeProject(
  id = "trembita-cassandra-connector",
  base = file("./cassandra_connector")
).dependsOn(kernel)
  .settings(libraryDependencies ++= {
    Seq("com.datastax.cassandra" % "cassandra-driver-core" % "3.4.0")
  })

lazy val cassandra_connector_phantom = sonatypeProject(
  id = "trembita-cassandra-connector-phantom",
  base = file("./cassandra_connector_phantom")
).dependsOn(cassandra_connector)
  .settings(libraryDependencies ++= {
    Seq(
      "com.outworkers" %% "phantom-jdk8" % "2.24.2",
      "com.datastax.cassandra" % "cassandra-driver-extras" % "3.4.0"
    )
  })

lazy val slf4j =
  sonatypeProject(id = "trembita-slf4j", base = file("./trembita-slf4j"))
    .dependsOn(kernel)
    .settings(libraryDependencies ++= {
      Seq("org.slf4j" % "slf4j-api" % "1.7.25")
    })

lazy val trembita_circe =
  sonatypeProject(id = "trembita-circe", base = file("./serialization/circe"))
    .dependsOn(kernel)
    .settings(
      name := "trembita-circe",
      version := v,
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
    collection_extentions,
    kernel,
    slf4j,
    trembita_circe,
    cassandra_connector,
    cassandra_connector_phantom
  )
  .settings(
    name := "trembita-examples",
    version := v,
    scalacOptions += "-Ypartial-unification",
    scalaVersion := scalaV,
    isSnapshot := snapshot,
    skip in publish := true,
    publish := {},
    publishLocal := {},
    addCompilerPlugin(
      "org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.full
    ),
    libraryDependencies ++= Seq("io.circe" %% "circe-java8" % "0.9.3")
  )

lazy val root = Project(id = "trembita", base = file("."))
  .aggregate(
    collection_extentions,
    kernel,
    slf4j,
    cassandra_connector,
    cassandra_connector_phantom,
    trembita_circe
  )
  .settings(
    name := "trembita",
    version := v,
    scalaVersion := scalaV,
    scalacOptions += "-Ypartial-unification",
    isSnapshot := snapshot,
    skip in publish := true,
    publish := {},
    publishLocal := {}
  )
