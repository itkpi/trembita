package com.github.trembita

import cats._
import cats.implicits._
import cats.effect._
import com.github.trembita.internal._
import org.scalatest.FlatSpec
import scala.util.Try

class KernelSpec extends FlatSpec {
  "DataPipeline operations" should "not be executed until 'eval'" in {
    val pipeline = DataPipeline(1, 2, 3)
    pipeline.map(_ => throw new Exception("Bang"))
    assert(true)
  }

  "DataPipeline.map(square)" should "be mapped squared" in {
    val pipeline = DataPipeline(1, 2, 3)
    val res: Vector[Int] = pipeline.map(i => i * i).eval
    assert(res == Vector(1, 4, 9))
  }

  "DataPipeline.filter(isEven)" should "contain only even numbers" in {
    val pipeline = DataPipeline(1, 2, 3)
    val res: Vector[Int] = pipeline.filter(_ % 2 == 0).eval
    assert(res == Vector(2))
  }

  "DataPipeline.collect(toInt)" should "be DataPipeline[Int]" in {
    val pipeline = DataPipeline("1", "2", "3", "abc")
    val res = pipeline.collect {
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
    val pipeline = DataPipeline("Hello world", "hello you to")
    val res = pipeline.flatMap(_.split("\\s")).eval
    assert(res == Vector("Hello", "world", "hello", "you", "to"))
  }

  "DataPipeline.sorted" should "be sorted" in {
    val pipeline = DataPipeline(5, 4, 3, 1)
    val sorted: DataPipeline[Int, Sequential] = pipeline.sorted
    assert(sorted.eval == Vector(1, 3, 4, 5))
  }

  "DataPipeline.sortBy(_.length)" should "be sorted by length" in {
    val pipeline = DataPipeline("a", "abcd", "bcd")
    val res: DataPipeline[String, Sequential] = pipeline.sortBy(_.length)
    assert(res.eval == Vector("a", "bcd", "abcd"))
  }

  "DataPipeline.reduce(_+_)" should "produce pipeline sum" in {
    val pipeline = DataPipeline(1, 2, 3)
    val res = pipeline.reduce(_ + _)
    assert(res == 6)
  }

  "DataPipeline.reduce" should "throw NoSuchElementException on empty pipeline" in {
    val pipeline = DataPipelineT.empty[cats.Id, Int]
    assertThrows[UnsupportedOperationException](pipeline.reduce(_ + _))
  }

  "DataPipeline.reduceOpt" should "work" in {
    val pipeline = DataPipeline(1, 2, 3)
    val res: Option[Int] = pipeline.reduceOpt(_ + _)
    assert(res.contains(6))
  }

  "DataPipeline.reduceOpt" should "produce None on empty pipeline" in {
    val pipeline = DataPipelineT.empty[cats.Id, Int]
    val res = pipeline.reduceOpt(_ + _)
    assert(res.isEmpty)
  }

  "DataPipeline.size" should "return pipeline size" in {
    val pipeline = DataPipeline(1, 2, 3)
    assert(pipeline.size == 3)
  }

  "DataPipeline.groupBy" should "group elements" in {
    val pipeline = DataPipeline(1, 2, 3, 4)
    val grouped: DataPipeline[(Boolean, List[Int]), Sequential] = pipeline
      .groupBy(_ % 2 == 0)
      .mapValues(_.toList)
      .sortBy(_._1)

    assert(grouped.eval == Vector(false -> List(1, 3), true -> List(2, 4)))
  }

  "DataPipeline.distinct" should "work" in {
    val pipeline = DataPipeline(1, 2, 3, 1, 3, 2, 1)
    val distinct = pipeline.distinct.eval.sorted
    assert(distinct == Vector(1, 2, 3))
  }

  "DataPipeline operations" should "be executed on each force" in {
    var x: Int = 0
    val pipeline = DataPipeline(1, 2, 3).map { i =>
      x += 1; i
    }
    val res1 = pipeline.eval
    assert(x == 3)
    val res2 = pipeline.eval
    assert(x == 6)
  }

  "split(n)" should "produce collection with n subcollections" in {
    val list = List(1, 2, 3, 4, 5, 6, 7, 8)
    val grouped = ListUtils.batch(4)(list).map(_.toList).toList
    assert(grouped == List(List(1, 2), List(3, 4), List(5, 6), List(7, 8)))
    val list2 = List(1, 2)
    val grouped2 = ListUtils.batch(4)(list2).map(_.toList).toList
    assert(grouped2 == List(List(1, 2)))
    val list3 = Nil
    val grouped3 = ListUtils.batch(2)(list3).map(_.toList).toList
    assert(grouped3 == Nil)
  }

  "PairPipeline transformations" should "work correctly" in {
    val pipeline = DataPipeline("a" → 1, "b" → 2, "c" → 3)
    val result1 = pipeline.mapValues(_ + 1).eval
    assert(result1 == Vector("a" → 2, "b" → 3, "c" → 4))

    val result2 = pipeline.keys.eval
    assert(result2 == Vector("a", "b", "c"))

    val result3 = pipeline.values.eval
    assert(result3 == Vector(1, 2, 3))
  }

  "PairPipeline.reduceByKey" should "produce correct result" in {
    val pipeline = DataPipeline("a" → 1, "b" → 2, "c" → 3, "a" → 3, "c" → 10)
    val result1 = pipeline.reduceByKey(_ + _).eval.sortBy(_._1)
    assert(result1 == Vector("a" → 4, "b" → 2, "c" → 13))

    val result2 = pipeline.reduceByKey.eval.sortBy(_._1) // uses Monoid
    assert(result1 == Vector("a" → 4, "b" → 2, "c" → 13))
  }

  "DataPipeline.zip" should "work correctly for pipelines" in {
    val p1 = DataPipeline(1, 2, 3, 4)
    val p2 = DataPipeline("a", "b", "c")
    val result = p1.zip(p2).eval
    assert(result == Vector(1 -> "a", 2 -> "b", 3 -> "c"))
  }

  "DataPipeline.++" should "work correctly for pipelines" in {
    val p1 = DataPipeline(1, 2, 3, 4)
    val p2 = DataPipeline(5, 6, 7)
    val result: DataPipeline[Int, Sequential] = (p1 ++ p2).sorted
    assert(result.eval == Vector(1, 2, 3, 4, 5, 6, 7))
  }

  "DataPipeline.take" should "work correctly" in {
    val pipeline = DataPipeline(1, 2, 3, 4)
    val result: DataPipeline[Int, Sequential] = pipeline.take(2)
    assert(result.eval == Vector(1, 2))
  }

  "DataPipeline.drop" should "work correctly" in {
    val pipeline = DataPipeline(1, 2, 3, 4)
    val result: DataPipeline[Int, Sequential] = pipeline.drop(2)
    assert(result.eval == Vector(3, 4))
  }

  "DataPipeline.slice" should "work correctly" in {
    val pipeline = DataPipeline(1, 2, 3, 4, 5)
    val result: DataPipeline[Int, Sequential] = pipeline.slice(1, 4)
    assert(result.eval == Vector(2, 3, 4))
  }

  "DataPipeline[IO]" should "produce the result wrapped in IO monad" in {
    val resultIO = DataPipelineT
      .liftF[IO, (String, Int), Environment.Sequential](
        IO(List("a" → 1, "b" → 2, "c" → 3, "a" → 3, "c" → 10))
      )
      .reduceByKey
      .mapValues(_ * 10)
      .map { case (k, v) => s"{key=$k, value=$v}" }
      .sorted
      .eval
      .map(_.toList.mkString(", "))

    assert(
      resultIO.unsafeRunSync() == "{key=a, value=40}, {key=b, value=20}, {key=c, value=130}"
    )
  }
}
