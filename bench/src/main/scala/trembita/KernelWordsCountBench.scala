package trembita

import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations._
import trembita.WordsCount.{ExtraLargeFile, LargeFile, MiddleFile, OhMyGoodnessFile}

@BenchmarkMode(Array(Mode.AverageTime))
class KernelWordsCountBench {
  @Benchmark
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def vectorMiddleFile(middleFile: MiddleFile): Unit = {
    val res = WordsCount.vector(middleFile.lines)
    println(res.size)
  }

  @Benchmark
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def vectorLargeFile(largeFile: LargeFile): Unit = {
    val res = WordsCount.vector(largeFile.lines)
    println(res.size)
  }

//  @Benchmark
//  @OutputTimeUnit(TimeUnit.MILLISECONDS)
//  def vectorExtraLargeFile(extraLargeFile: ExtraLargeFile): Unit = {
//    val res = WordsCount.vector(extraLargeFile.lines)
//    println(res.size)
//  }

//  @Benchmark
//  @OutputTimeUnit(TimeUnit.MILLISECONDS)
//  def vectorOhMyGoodness(ohMyGoodness: OhMyGoodnessFile): Unit = {
//    val res = WordsCount.vector(ohMyGoodness.ohNooo)
//    println(res.size)
//  }
  @Benchmark
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def imperativeMiddleFile(middleFile: MiddleFile): Unit = {
    val res = WordsCount.imperative(middleFile.lines)
    println(res.size)
  }

  @Benchmark
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def imperativeLargeFile(largeFile: LargeFile): Unit = {
    val res = WordsCount.imperative(largeFile.lines)
    println(res.size)
  }
//
//  @Benchmark
//  @OutputTimeUnit(TimeUnit.MILLISECONDS)
//  def imperativeExtraLargeFile(extraLargeFile: ExtraLargeFile): Unit = {
//    val res = WordsCount.imperative(extraLargeFile.lines)
//    println(res.size)
//  }

//  @Benchmark
//  @OutputTimeUnit(TimeUnit.MILLISECONDS)
//  def imperativeOhMyGoodnessFile(ohMyGoodness: OhMyGoodnessFile): Unit = {
//    val res = WordsCount.imperative(ohMyGoodness.ohNooo)
//    println(res.size)
//  }

  @Benchmark
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def pipelineNaiveMiddleFile(middleFile: MiddleFile): Unit = {
    val res = WordsCount.pipelineNaive(middleFile.lines)
    println(res.size)
  }

  @Benchmark
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def pipelineNaiveLargeFile(largeFile: LargeFile): Unit = {
    val res = WordsCount.pipelineNaive(largeFile.lines)
    println(res.size)
  }

//  @Benchmark
//  @OutputTimeUnit(TimeUnit.MILLISECONDS)
//  def pipelineNaiveExtraLargeFile(extraLargeFile: ExtraLargeFile): Unit = {
//    val res = WordsCount.pipelineNaive(extraLargeFile.lines)
//    println(res.size)
//  }

//  @Benchmark
//  @OutputTimeUnit(TimeUnit.MILLISECONDS)
//  def pipelineNaiveOhMyGoodness(ohMyGoodness: OhMyGoodnessFile): Unit = {
//    val res = WordsCount.pipelineNaive(ohMyGoodness.ohNooo)
//    println(res.size)
//  }

  @Benchmark
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def pipelineAdvancedMiddleFile(middleFile: MiddleFile): Unit = {
    val res = WordsCount.pipelinesAdvanced(middleFile.lines)
    println(res.size)
  }

  @Benchmark
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def pipelinesAdvancedLargeFile(largeFile: LargeFile): Unit = {
    val res = WordsCount.pipelinesAdvanced(largeFile.lines)
    println(res.size)
  }
//
//  @Benchmark
//  @OutputTimeUnit(TimeUnit.MILLISECONDS)
//  def pipelinesAdvanceExtraLargeFile(extraLargeFile: ExtraLargeFile): Unit = {
//    val res = WordsCount.pipelinesAdvanced(extraLargeFile.lines, parallelism = 16)
//    println(res.size)
//  }
//
//  @Benchmark
//  @OutputTimeUnit(TimeUnit.MILLISECONDS)
//  def pipelinesAdvanceOhMyGoodness(ohMyGoodness: OhMyGoodnessFile): Unit = {
//    val res = WordsCount.pipelinesAdvanced(ohMyGoodness.ohNooo, parallelism = 16)
//    println(res.size)
//  }
}
