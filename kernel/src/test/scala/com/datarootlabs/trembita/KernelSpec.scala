package com.datarootlabs.trembita

import internal._
import org.scalatest.FlatSpec
import cats.implicits._
import cats.effect._
import scala.util.Try


class KernelSpec extends FlatSpec {
  "DataPipeline operations" should "not be executed until 'eval'" in {
    val list = DataPipeline(1, 2, 3)
    list.map(_ => throw new Exception("Bang"))
    assert(true)
  }

  "DataPipeline.map(square)" should "be mapped squared" in {
    val list = DataPipeline(1, 2, 3)
    val res: Try[Vector[Int]] = list.map(i => i * i).eval
    assert(res.get == Vector(1, 4, 9))
  }

  "DataPipeline.map(square).evalAs[List]" should "produce a list of squares" in {
    val list = DataPipeline(1, 2, 3)
    val res: Try[List[Int]] = list.map(i => i * i).evalAs[List]
    assert(res.get == List(1, 4, 9))
  }

  "DataPipeline.filter(isEven)" should "contain only even numbers" in {
    val list = DataPipeline(1, 2, 3)
    val res: Try[List[Int]] = list.filter(_ % 2 == 0).evalAs[List]
    assert(res.get == List(2))
  }

  "DataPipeline.collect(toInt)" should "be DataPipeline[Int]" in {
    val list = DataPipeline("1", "2", "3", "abc")
    val res = list.collect {
      case str if str.forall(_.isDigit) => str.toInt
    }.eval
    assert(res.get == Vector(1, 2, 3))
  }

  "DataPipeline[String, Try, ...].mapM(...toInt)" should "be DataPipeline[Int]" in {
    val list = DataPipeline("1", "2", "3", "abc")
    val res = list.mapM { str ⇒ Try(str.toInt) }.handleError { case e: NumberFormatException => -10 }.eval
    assert(res.get == Vector(1, 2, 3, -10))
  }

  "DataPipeline.flatMap(getWords)" should "be a list of words" in {
    val list = DataPipeline("Hello world", "hello you to")
    val res = list.flatMap(_.split("\\s")).eval
    assert(res.get == Vector("Hello", "world", "hello", "you", "to"))
  }

  "DataPipeline.sorted" should "be sorted" in {
    val list = DataPipeline(5, 4, 3, 1)
    val res = list.sorted.eval
    assert(res.get == Vector(1, 3, 4, 5))
  }

  "DataPipeline.sortBy(_.length)" should "be sorted by length" in {
    val list = DataPipeline("a", "abcd", "bcd")
    val res = list.sortBy(_.length).eval
    assert(res.get == Vector("a", "bcd", "abcd"))
  }

  "DataPipeline.reduce(_+_)" should "produce list sum" in {
    val list = DataPipeline(1, 2, 3)
    val res = list.reduce(_ + _)
    assert(res.get == 6)
  }

  "DataPipeline.reduce" should "throw NoSuchElementException on empty list" in {
    val list = DataPipeline.empty[Int]
    assertThrows[NoSuchElementException](list.reduce(_ + _).get)
  }

  "DataPipeline.reduceOpt" should "work" in {
    val list = DataPipeline(1, 2, 3)
    val res = list.reduceOpt(_ + _)
    assert(res.get.contains(6))
  }

  "DataPipeline.reduceOpt" should "produce None on empty list" in {
    val list = DataPipeline.empty[Int]
    val res = list.reduceOpt(_ + _).get
    assert(res.isEmpty)
  }

  "DataPipeline.size" should "return list size" in {
    val list = DataPipeline(1, 2, 3)
    assert(list.size.get == 3)
  }

  "DataPipeline.groupBy" should "group elements" in {
    val list = DataPipeline(1, 2, 3, 4)
    val grouped = list.groupBy(_ % 2 == 0)
      .mapValues(_.toList)
      .sortBy(_._1)
      .eval

    assert(grouped.get == Vector(false -> List(1, 3), true -> List(2, 4)))
  }

  "DataPipeline.distinct" should "work" in {
    val list = DataPipeline(1, 2, 3, 1, 3, 2, 1)
    val distinct = list.distinct.eval.get.sorted
    assert(distinct == Vector(1, 2, 3))
  }

  "DataPipeline operations" should "be executed on each force" in {
    var x: Int = 0
    val list = DataPipeline(1, 2, 3).map { i => x += 1; i }
    val res1 = list.eval
    assert(x == 3)
    val res2 = list.eval
    assert(x == 6)
  }

  "DataPipeline operations after .memoize()" should "be executed exactly once" in {
    var x: Int = 0
    val list = DataPipeline(1, 2, 3).map { i => x += 1; i }.memoize()
    val res1 = list.eval
    assert(x == 3)
    val res2 = list.eval
    assert(x == 3)
  }

  "DataPipeline.find" should "successfully find an element" in {
    val list = DataPipeline(1, 2, 3)
    assert(list.find(_ % 2 == 0).get contains 2)
  }

  "DataPipeline.contains" should "work correctly" in {
    val pipeline = DataPipeline(1, 2, 3)
    assert(pipeline.contains(2).get)
    assert(!pipeline.contains(5).get)
  }

  "DataPipeline.forall" should "work correctly" in {
    val pipeline = DataPipeline(1, 2, 3)
    assert(pipeline.forall(_ > 0).get)
    assert(!pipeline.forall(_ % 2 == 0).get)
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
    val result1 = pipeline.mapValues(_ + 1).eval.get
    assert(result1 == Vector("a" → 2, "b" → 3, "c" → 4))

    val result2 = pipeline.keys.eval.get
    assert(result2 == Vector("a", "b", "c"))

    val result3 = pipeline.values.eval.get
    assert(result3 == Vector(1, 2, 3))

    //    val result4 = (pipeline :+ ("a" → 2)).toMap.keys.eval.get.sorted
    //    assert(result4 == Vector("a", "b", "c"))
  }

  "PairPipeline.reduceByKey" should "produce correct result" in {
    val pipeline = DataPipeline("a" → 1, "b" → 2, "c" → 3, "a" → 3, "c" → 10)
    val result1 = pipeline.reduceByKey(_ + _).eval.get.sortBy(_._1)
    assert(result1 == Vector("a" → 4, "b" → 2, "c" → 13))

    val result2 = pipeline.reduceByKey.eval.get.sortBy(_._1) // uses Monoid
    assert(result1 == Vector("a" → 4, "b" → 2, "c" → 13))
  }

  "DataPipeline[IO]" should "produce the result wrapped in IO monad" in {
    val resultIO = DataPipeline.fromEffect[(String, Int), IO, Finiteness.Finite, Execution.Sequential](
      IO(List("a" → 1, "b" → 2, "c" → 3, "a" → 3, "c" → 10))
    )
      .reduceByKey
      .mapValues(_ * 10)
      .map { case (k, v) ⇒ s"{key=$k, value=$v}" }
      .sorted
      .eval.map(_.toList.mkString(", "))

    assert(resultIO.unsafeRunSync() == "{key=a, value=40}, {key=b, value=20}, {key=c, value=130}")
  }
}
