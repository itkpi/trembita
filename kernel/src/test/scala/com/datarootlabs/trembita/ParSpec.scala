package com.datarootlabs.trembita


import java.util.concurrent.atomic.AtomicInteger
import parallel._
import org.scalatest.FlatSpec
import cats.implicits._
import cats.effect._
import scala.util.Try
import scala.concurrent.ExecutionContext.Implicits.global


class ParSpec extends FlatSpec {
  "ParDataPipeline operations" should "not be executed until 'eval'" in {
    val pipeline = DataPipeline(1, 2, 3).par
    pipeline.map(_ => throw new Exception("Bang"))
    assert(true)
  }

  "ParDataPipeline[Int].map(square)" should "be mapped squared" in {
    val pipeline: ParDataPipeline[Int] = DataPipeline(1, 2, 3).par
    val res: List[Int] = pipeline.map(i => i * i).evalAs[List].sorted
    assert(res == List(1, 4, 9))
  }

  "ParDataPipeline[Int].map(square).evalAs[List]" should "produce a pipeline of squares" in {
    val pipeline: ParDataPipeline[Int] = DataPipeline(1, 2, 3).par
    val res: List[Int] = pipeline.map(i => i * i).evalAs[List].sorted
    assert(res == List(1, 4, 9))
  }

  "ParDataPipeline[Int].filter(isEven)" should "contain only even numbers" in {
    val pipeline: DataPipeline[Int] = DataPipeline(1, 2, 3).par
    val res: List[Int] = pipeline.filter(_ % 2 == 0).evalAs[List].sorted
    assert(res == List(2))
  }

  "ParDataPipeline[String].collect(toInt)" should "be ParDataPipeline[Int]" in {
    val pipeline: DataPipeline[String] = DataPipeline("1", "2", "3", "abc").par
    val res = pipeline.collect {
      case str if str.forall(_.isDigit) => str.toInt
    }.evalAs[List].sorted
    assert(res == List(1, 2, 3))
  }

  "ParDataPipeline[String].tryMap(...toInt)" should "be ParDataPipeline[Int]" in {
    val pipeline: DataPipeline[String] = DataPipeline("1", "2", "3", "abc").par
    val res = pipeline.tryMap { str ⇒ Try(str.toInt) }.evalAs[List].sorted
    assert(res == List(1, 2, 3))
  }

  "ParDataPipeline[String].flatMap(getWords)" should "be a pipeline of words" in {
    val pipeline: DataPipeline[String] = DataPipeline("Hello world", "hello you to").par
    val res = pipeline.flatMap(_.split("\\s")).evalAs[List].sorted
    assert(res == List("Hello", "hello", "to", "world", "you"))
  }

  "ParDataPipeline[Int].sorted" should "be sorted" in {
    val pipeline: DataPipeline[Int] = DataPipeline(5, 4, 3, 1).par
    val res = pipeline.sorted.evalAs[List]
    assert(res == List(1, 3, 4, 5))
  }

  "ParDataPipeline[String]" should "be sorted by length" in {
    val pipeline = DataPipeline("a", "abcd", "bcd")
    val res = pipeline.sortBy(_.length).evalAs[List]
    assert(res == List("a", "bcd", "abcd"))
  }

  "ParDataPipeline[Int].reduce(_+_)" should "produce pipeline sum" in {
    val pipeline = DataPipeline(1, 2, 3).par
    val res = pipeline.reduce(_ + _)
    assert(res == 6)
  }

  "ParDataPipeline[Int].reduce" should "throw NoSuchElementException on empty pipeline" in {
    val pipeline = DataPipeline.empty[Int].par
    assertThrows[NoSuchElementException](pipeline.reduce(_ + _))
  }

  "ParDataPipeline[Int].reduceOpt" should "work" in {
    val pipeline = DataPipeline(1, 2, 3).par
    val res = pipeline.reduceOpt(_ + _)
    assert(res.contains(6))
  }

  "ParDataPipeline[Int].reduceOpt" should "produce None on empty pipeline" in {
    val pipeline = DataPipeline.empty[Int].par
    val res = pipeline.reduceOpt(_ + _)
    assert(res.isEmpty)
  }

  "ParDataPipeline.size" should "return pipeline size" in {
    val pipeline = DataPipeline(1, 2, 3).par
    assert(pipeline.size == 3)
  }

  "ParDataPipeline.groupBy" should "group elements" in {
    val pipeline = DataPipeline(1, 2, 3, 4).par
    val grouped = pipeline.groupBy(_ % 2 == 0)
      .mapValues(_.toList)
      .sortBy(_._1)
      .evalAs[List]

    assert(grouped == List(false -> List(1, 3), true -> List(2, 4)))
  }

  "ParDataPipeline.distinct" should "work" in {
    val pipeline = DataPipeline(1, 2, 3, 1, 3, 2, 1).par
    val distinct = pipeline.distinct.evalAs[List].sorted
    assert(distinct == List(1, 2, 3))
  }

  "ParDataPipeline operations" should "be executed on each eval" in {
    var x: AtomicInteger = new AtomicInteger(0)
    val pipeline = DataPipeline(1, 2, 3).par.map { i => x.incrementAndGet(); i }
    val res1 = pipeline.eval
    assert(x.get() == 3)
    val res2 = pipeline.eval
    assert(x.get() == 6)
  }

  "ParDataPipeline operations after .cache()" should "be executed exactly once" in {
    var x: AtomicInteger = new AtomicInteger(0)
    val pipeline = DataPipeline(1, 2, 3).par.map { i => x.incrementAndGet(); i }.cache()
    val res1 = pipeline.eval
    assert(x.get() == 3)
    val res2 = pipeline.eval
    assert(x.get() == 3)
  }

  "ParDataPipeline.find" should "successfully find an element" in {
    val pipeline = DataPipeline(1, 2, 3).par
    assert(pipeline.find(_ % 2 == 0) contains 2)
  }

  "ParDataPipeline.contains" should "work correctly" in {
    val pipeline = DataPipeline(1, 2, 3).par
    assert(pipeline.contains(2))
    assert(!pipeline.contains(5))
  }

  "ParDataPipeline.forall" should "work correctly" in {
    val pipeline = DataPipeline(1, 2, 3).par
    assert(pipeline.forall(_ > 0))
    assert(!pipeline.forall(_ % 2 == 0))
  }

  "PairPipeline transformations" should "work correctly" in {
    val pipeline = DataPipeline("a" → 1, "b" → 2, "c" → 3).par
    val result1 = pipeline.mapValues(_ + 1).evalAs[List].sortBy(_._1)
    assert(result1 == List("a" → 2, "b" → 3, "c" → 4))

    val result2 = pipeline.keys.evalAs[List].sorted
    assert(result2 == List("a", "b", "c"))

    val result3 = pipeline.values.evalAs[List].sorted
    assert(result3 == List(1, 2, 3))

    val result4 = (pipeline :+ ("a" → 2)).toMap.keys.evalAs[List].sorted
    assert(result4 == List("a", "b", "c"))
  }

  "PairPipeline.reduceByKey" should "produce correct result" in {
    val pipeline = DataPipeline("a" → 1, "b" → 2, "c" → 3, "a" → 3, "c" → 10).par
    val result1 = pipeline.reduceByKey(_ + _).evalAs[List].sortBy(_._1)
    assert(result1 == List("a" → 4, "b" → 2, "c" → 13))

    val result2 = pipeline.reduceByKey.evalAs[List].sortBy(_._1) // uses Monoid
    assert(result2 == List("a" → 4, "b" → 2, "c" → 13))

    // @formatter:off
      val result3 = DataPipeline("a" → 1, "b" → 2, "c" → 3)
        .par
        .combine(
          init = 0: Int,
          add = { (acc: Int, pair: (String, Int)) ⇒ acc + pair._2 },
          merge = (_: Int) + (_: Int)
        )

      assert(result3 == 6)

      val result4 = DataPipeline("a" → 1, "b" → 2, "c" → 3)
        .par.combine((_: Int) + _._2) // uses Monoid

      assert(result4 == 6)
      // @formatter:on
  }

  "ParDataPipeline.run[IO]" should "produce the result wrapped in IO monad" in {
    val resultIO = DataPipeline("a" → 1, "b" → 2, "c" → 3, "a" → 3, "c" → 10).par
      .reduceByKey
      .mapValues(_ * 10)
      .map { case (k, v) ⇒ s"{key=$k, value=$v}" }
      .sorted
      .run[IO].map(_.toList.mkString(", "))

    assert(resultIO.unsafeRunSync() == "{key=a, value=40}, {key=b, value=20}, {key=c, value=130}")
  }
}
