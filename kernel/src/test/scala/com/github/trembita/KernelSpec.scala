package com.github.trembita

import cats._
import cats.data.Kleisli
import cats.implicits._
import cats.effect._
import com.github.trembita.internal.ListUtils
import org.scalatest.FlatSpec

import scala.util.Try
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

class KernelSpec extends FlatSpec {
  implicit val contextShift: ContextShift[IO] = IO.contextShift(global)
  implicit val ioTimer: Timer[IO]             = IO.timer(global)

  "DataPipeline operations" should "not be executed until 'eval'" in {
    val pipeline = DataPipeline(1, 2, 3)
    pipeline.map(_ => throw new Exception("Bang"))
    assert(true)
  }

  "DataPipeline.map(square)" should "be mapped squared" in {
    val pipeline         = DataPipeline(1, 2, 3)
    val res: Vector[Int] = pipeline.map(i => i * i).eval
    assert(res == Vector(1, 4, 9))
  }

  "DataPipeline.filter(isEven)" should "contain only even numbers" in {
    val pipeline         = DataPipeline(1, 2, 3)
    val res: Vector[Int] = pipeline.filter(_ % 2 == 0).eval
    assert(res == Vector(2))
  }

  "DataPipeline.collect(toInt)" should "be DataPipeline[Int]" in {
    val pipeline = DataPipeline("1", "2", "3", "abc")
    val res: Vector[Int] = pipeline.collect {
      case str if str.forall(_.isDigit) => str.toInt
    }.eval
    assert(res == Vector(1, 2, 3))
  }

  "DataPipeline[String, Try, ...].mapM(...toInt)" should "be DataPipeline[Int]" in {
    val pipeline = DataPipelineT[Try, String]("1", "2", "3", "abc")
    val res = pipeline
      .mapM { str =>
        Try(str.toInt)
      }
      .recover { case e: NumberFormatException => -10 }
      .eval
    assert(res.get == Vector(1, 2, 3, -10))
  }

  "DataPipeline.flatMap(getWords)" should "be a pipeline of words" in {
    val pipeline            = DataPipeline("Hello world", "hello you to")
    val res: Vector[String] = pipeline.mapConcat(_.split("\\s")).eval
    assert(res == Vector("Hello", "world", "hello", "you", "to"))
  }

  "DataPipeline.sorted" should "be sorted" in {
    val pipeline            = DataPipeline(5, 4, 3, 1)
    val sorted: Vector[Int] = pipeline.sorted.eval
    assert(sorted == Vector(1, 3, 4, 5))
  }

  "DataPipeline.sortBy(_.length)" should "be sorted by length" in {
    val pipeline            = DataPipeline("a", "abcd", "bcd")
    val res: Vector[String] = pipeline.sortBy(_.length).eval
    assert(res == Vector("a", "bcd", "abcd"))
  }

  "DataPipeline.reduce(_+_)" should "produce pipeline sum" in {
    val pipeline = DataPipeline(1, 2, 3)
    val res: Int = pipeline.reduce(_ + _)
    assert(res == 6)
  }

  "DataPipeline.reduce" should "throw NoSuchElementException on empty pipeline" in {
    val pipeline = DataPipelineT.empty[Id, Int]
    assertThrows[UnsupportedOperationException] {
      val result: Int = pipeline.reduce(_ + _)
      result
    }
  }

  "DataPipeline.reduceOpt" should "work" in {
    val pipeline         = DataPipeline(1, 2, 3)
    val res: Option[Int] = pipeline.reduceOpt(_ + _)
    assert(res.contains(6))
  }

  "DataPipeline.reduceOpt" should "produce None on empty pipeline" in {
    val pipeline         = DataPipelineT.empty[Id, Int]
    val res: Option[Int] = pipeline.reduceOpt(_ + _)
    assert(res.isEmpty)
  }

  "DataPipeline.size" should "return pipeline size" in {
    val pipeline  = DataPipeline(1, 2, 3)
    val size: Int = pipeline.size
    assert(size == 3)
  }

  "DataPipeline.groupBy" should "group elements" in {
    val pipeline = DataPipeline(1, 2, 3, 4)
    val grouped: Vector[(Boolean, List[Int])] = pipeline
      .groupBy(_ % 2 == 0)
      .mapValues(_.toList)
      .sortBy(_._1)
      .eval

    assert(grouped == Vector(false -> List(1, 3), true -> List(2, 4)))
  }

  "DataPipeline.distinct" should "work" in {
    val pipeline              = DataPipeline(1, 2, 3, 1, 3, 2, 1)
    val distinct: Vector[Int] = pipeline.distinct.eval
    assert(distinct.sorted == Vector(1, 2, 3))
  }

  "DataPipeline operations" should "be executed on each force" in {
    var x: Int = 0
    val pipeline = DataPipeline(1, 2, 3).map { i =>
      x += 1; i
    }
    val res1: Vector[Int] = pipeline.eval
    assert(x == 3)
    val res2: Vector[Int] = pipeline.eval
    assert(x == 6)
  }

  "split(n)" should "produce collection with n subcollections" in {
    val list    = List(1, 2, 3, 4, 5, 6, 7, 8)
    val grouped = ListUtils.batch(4)(list).map(_.toList).toList
    assert(grouped == List(List(1, 2), List(3, 4), List(5, 6), List(7, 8)))
    val list2    = List(1, 2)
    val grouped2 = ListUtils.batch(4)(list2).map(_.toList).toList
    assert(grouped2 == List(List(1, 2)))
    val list3    = Nil
    val grouped3 = ListUtils.batch(2)(list3).map(_.toList).toList
    assert(grouped3 == Nil)
  }

  "PairPipeline transformations" should "work correctly" in {
    val pipeline = DataPipeline("a" → 1, "b" → 2, "c" → 3)

    val result1: Vector[(String, Int)] = pipeline.mapValues(_ + 1).eval
    assert(result1 == Vector("a" → 2, "b" → 3, "c" → 4))

    val result2: Vector[String] = pipeline.keys.eval
    assert(result2 == Vector("a", "b", "c"))

    val result3: Vector[Int] = pipeline.values.eval
    assert(result3 == Vector(1, 2, 3))
  }

  "DataPipeline.zip" should "work correctly for pipelines" in {
    val p1                            = DataPipeline(1, 2, 3, 4)
    val p2                            = DataPipeline("a", "b", "c")
    val result: Vector[(Int, String)] = p1.zip(p2).eval
    assert(result == Vector(1 -> "a", 2 -> "b", 3 -> "c"))
  }

  "DataPipeline.++" should "work correctly for pipelines" in {
    val p1                  = DataPipeline(1, 2, 3, 4)
    val p2                  = DataPipeline(5, 6, 7)
    val result: Vector[Int] = (p1 ++ p2).sorted.eval
    assert(result == Vector(1, 2, 3, 4, 5, 6, 7))
  }

  "DataPipeline.take" should "work correctly" in {
    val pipeline            = DataPipeline(1, 2, 3, 4)
    val result: Vector[Int] = pipeline.take(2).eval
    assert(result == Vector(1, 2))
  }

  "DataPipeline.drop" should "work correctly" in {
    val pipeline            = DataPipeline(1, 2, 3, 4)
    val result: Vector[Int] = pipeline.drop(2).eval
    assert(result == Vector(3, 4))
  }

  "DataPipeline.slice" should "work correctly" in {
    val pipeline            = DataPipeline(1, 2, 3, 4, 5)
    val result: Vector[Int] = pipeline.slice(1, 4).eval
    assert(result == Vector(2, 3, 4))
  }

  "DataPipeline[IO]" should "produce the result wrapped in IO monad" in {
    val resultIO = DataPipelineT
      .liftF[IO, (String, Int), Environment.Sequential](
        IO(List("a" → 1, "b" → 2, "c" → 3, "a" → 3, "c" → 10))
      )
      .groupBy(_._1)
      .mapValues(_.foldLeft(0) { case (acc, (_, x)) => acc + x } * 10)
      .map { case (k, v) => s"{key=$k, value=$v}" }
      .sorted
      .eval
      .map(_.toList.mkString(", "))

    assert(
      resultIO.unsafeRunSync() == "{key=a, value=40}, {key=b, value=20}, {key=c, value=130}"
    )
  }

  "DataPipeline of IO" should "be paused correctly" in {
    val pipeline  = DataPipelineT[IO, Int](1, 2, 3, 4, 5)
    val paused    = pipeline.pausedWith(_.seconds)
    val startTime = System.currentTimeMillis()
    val evaled    = paused.eval.unsafeRunSync()
    val endTime   = System.currentTimeMillis()
    assert(evaled == Vector(1, 2, 3, 4, 5))
    assert((endTime - startTime).millis >= evaled.sum.seconds)
  }

  "DataPipeline of IO" should "be paused correctly with CanPause2" in {
    val pipeline  = DataPipelineT[IO, Int](1, 2, 3, 4, 5)
    val paused    = pipeline.pausedWith2((a, b) => (b - a).seconds)
    val startTime = System.currentTimeMillis()
    val evaled    = paused.eval.unsafeRunSync()
    val endTime   = System.currentTimeMillis()
    assert(evaled == Vector(1, 2, 3, 4, 5))
    assert((endTime - startTime).millis >= 4.seconds)
  }

  "DataPipeline transformations wrapped in Kleisli" should "be evaluated correctly" in {
    val pipeline = DataPipelineT[IO, Int](1, 2, 3, 4, 5)
    val transformations = Kleisli[DataPipelineT[IO, ?, Sequential], DataPipelineT[IO, Int, Sequential], String](
      _.mapM(i => IO { i + 1 })
        .filter(_ % 2 == 0)
        .map(_.toString)
    )

    val evaled = pipeline.through(transformations).eval.unsafeRunSync()

    assert(evaled == Vector("2", "4", "6"))
  }
}
