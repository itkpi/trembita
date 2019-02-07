package trembita

import java.nio.file.{FileSystems, Path}
import org.openjdk.jmh.annotations._
import scala.io.Source

object WordsCountFiles {
  val middleFile: Path       = FileSystems.getDefault.getPath("bench/src/main/resources/middle.txt").toAbsolutePath
  val largeFile: Path        = FileSystems.getDefault.getPath("bench/src/main/resources/big.txt").toAbsolutePath
  val extraLargeFile: String = "https://storage.googleapis.com/google-code-archive-downloads/v2/code.google.com/jquery-speedtest/100MB.txt"
}

object WordsCount {
  @State(Scope.Benchmark)
  class MiddleFile {
    var lines: Vector[String] = _

    @Setup
    def init(): Unit =
      lines = Source.fromFile(WordsCountFiles.middleFile.toFile).getLines().toVector
  }

  @State(Scope.Benchmark)
  class LargeFile {
    var lines: Vector[String] = _

    @Setup
    def init(): Unit =
      lines = Source.fromFile(WordsCountFiles.largeFile.toFile).getLines().toVector
  }

  @State(Scope.Benchmark)
  class ExtraLargeFile {
    var lines: Vector[String] = _

    @Setup
    def init(): Unit =
      lines = Source.fromURL(WordsCountFiles.extraLargeFile).getLines().toVector
  }

  @State(Scope.Benchmark)
  class OhMyGoodnessFile {
    var ohNooo: Vector[String] = _

    @Setup
    def init(): Unit =
      ohNooo = Source
        .fromURL(WordsCountFiles.extraLargeFile)
        .getLines()
        .toVector
        .flatMap { line =>
          (1 to 10).map(_ => line)
        }
  }

  def pipelineNaive(lines: Vector[String]): Vector[(String, Int)] =
    Input
      .sequential[Vector]
      .create(lines)
      .mapConcat(_.split("\\W+"))
      .groupByKey(identity)
      .mapValues(_.size)
      .into(Output.vector)
      .run

  def pipelinesAdvanced(lines: Vector[String]): Vector[(String, Int)] =
    Input
      .parallel[Vector]
      .create(lines)
      .mapConcat(_.split("\\W+"))
      .map(_ -> 1)
      .combineByKey[Int]((a: Int) => a, (comb: Int, a: Int) => comb + a, (comb1: Int, comb2: Int) => comb1 + comb2)
      .into(Output.vector)
      .run

  def pipelinesAdvanced(lines: Vector[String], parallelism: Int): Vector[(String, Int)] =
    Input
      .parallel[Vector]
      .create(lines)
      .mapConcat(_.split("\\W+"))
      .map(_ -> 1)
      .combineByKey[Int](parallelism)((a: Int) => a, (comb: Int, a: Int) => comb + a, (comb1: Int, comb2: Int) => comb1 + comb2)
      .into(Output.vector)
      .run

  def vector(lines: Vector[String]): Vector[(String, Int)] =
    lines
      .flatMap(_.split("\\W+"))
      .groupBy(identity)
      .mapValues(_.size)
      .toVector

  def imperative(lines: Vector[String]): Vector[(String, Int)] = {
    var map = Map.empty[String, Int]

    for (line <- lines) {
      for (word <- line split "\\W+") {
        if (map contains word) map = map.updated(word, map(word) + 1)
        else map += (word -> 1)
      }
    }

    map.toVector
  }
}
