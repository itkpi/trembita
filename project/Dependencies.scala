import sbt._
import sbt.Keys.{crossScalaVersions, libraryDependencies, scalaVersion, scalacOptions}

object Dependencies {
  val `scala-2.12`         = "2.12.10"
  val `scala-2.13`         = "2.13.1"
  val parallelCollectionsV = "0.2.0"
  val testV                = "3.1.0"
  val catsEffectsV         = "2.0.0"
  val shapelessV           = "2.3.3"
  val spireV               = "0.17.0-M1"
  val slf4jV               = "1.7.25"
  val sparkV               = "2.4.4"
  val akkaV                = "2.6.1"
  val akkaHttpV            = "10.1.11"
  val alpakkaV             = "1.1.2"
  val scalaJava8CompatV    = "0.9.0"
  val mockitoV             = "2.23.4"
  val sttpV                = "1.6.4"
  val zioV                 = "1.0.0-RC17"

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

  object Utils {
    val slf4j = "org.slf4j" % "slf4j-api" % slf4jV withSources ()
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

  object ScalaCompat {
    val java8compat = "org.scala-lang.modules" %% "scala-java8-compat" % scalaJava8CompatV withSources ()
  }

  object Sttp {
    val core        = "com.softwaremill.sttp" %% "core"                           % sttpV
    val asyncHttp   = "com.softwaremill.sttp" %% "async-http-client-backend"      % sttpV
    val catsBackend = "com.softwaremill.sttp" %% "async-http-client-backend-cats" % sttpV
  }

  val kindProjector = "org.typelevel" %% "kind-projector" % "0.11.0" cross CrossVersion.full

  private def onScalaVersion[U](`on-2-12`: => U, `on-2-13`: => U): Def.Initialize[U] = Def.setting {
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, 13)) => `on-2-13`
      case _             => `on-2-12`
    }
  }

  implicit class ProjectOps(private val self: Project) extends AnyVal {
    def withMacroAnnotations(): Project = {
      val options = onScalaVersion(
        `on-2-12` = List(compilerPlugin("org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.patch)) -> Nil,
        `on-2-13` = Nil -> List("-Ymacro-annotations")
      )
      self.settings(
        libraryDependencies ++= options.value._1,
        scalacOptions ++= options.value._2
      )
    }

    def withCommonSettings(): Project = {
      val option = onScalaVersion(
        `on-2-12` = List("-Ypartial-unification"),
        `on-2-13` = Nil
      )
      val baseDeps = List(
        Testing.scalastic,
        Testing.scalatest % "test",
        Typelevel.catsEffect,
        Typelevel.shapeless,
        Typelevel.spire
      )
      val parallelCollections = onScalaVersion(
        `on-2-12` = Nil,
        `on-2-13` = List("org.scala-lang.modules" %% "scala-parallel-collections" % parallelCollectionsV)
      )
      self.settings(
        scalacOptions ++= option.value ++ List(
          "-feature"
        ),
        libraryDependencies ++= baseDeps ++ parallelCollections.value,
        addCompilerPlugin(kindProjector)
      )
    }

    def scala212Only(): Project =
      self.settings(
        scalaVersion := `scala-2.12`,
        crossScalaVersions := Seq(`scala-2.12`)
      )

    def withSpark(): Project = {
      val deps = onScalaVersion(
        `on-2-12` = List(
          Spark.core,
          Spark.sql,
          Spark.streaming
        ),
        `on-2-13` = Nil
      )
      self.settings(
        libraryDependencies ++= deps.value
      )
    }

  }
}
