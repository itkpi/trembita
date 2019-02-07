package trembita

import cats._
import cats.implicits._
import cats.effect._
import trembita.internal.BatchUtils
import org.scalatest.FlatSpec
import scala.util.Try
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

class SequentialSpec extends FlatSpec {
  implicit val contextShift: ContextShift[IO] = IO.contextShift(global)
  implicit val ioTimer: Timer[IO]             = IO.timer(global)

  "DataPipeline operations" should "not be executed until 'eval'" in {
    val pipeline = Input.sequential[Seq].create(Seq(1, 2, 3))
    pipeline.map(_ => throw new Exception("Bang"))
    assert(true)
  }

  "DataPipeline.map(square)" should "be mapped squared" in {
    val pipeline         = Input.sequential[Seq].create(Seq(1, 2, 3))
    val res: Vector[Int] = pipeline.map(i => i * i).into(Output.vector).run
    assert(res == Vector(1, 4, 9))
  }

  "DataPipeline.filter(isEven)" should "contain only even numbers" in {
    val pipeline         = Input.sequential[Seq].create(Seq(1, 2, 3))
    val res: Vector[Int] = pipeline.filter(_ % 2 == 0).into(Output.vector).run
    assert(res == Vector(2))
  }

  "DataPipeline.collect(toInt)" should "be DataPipeline[Int]" in {
    val pipeline = Input.sequential[Seq].create(Seq("1", "2", "3", "abc"))
    val res: Vector[Int] = pipeline
      .collect {
        case str if str.forall(_.isDigit) => str.toInt
      }
      .into(Output.vector)
      .run
    assert(res == Vector(1, 2, 3))
  }

  "DataPipeline[String, Try, ...].mapM(...toInt)" should "be DataPipeline[Int]" in {
    val pipeline = Input.sequentialF[Try, Seq].create(Try(Vector("1", "2", "3", "abc")))
    val res = pipeline
      .mapM { str =>
        Try(str.toInt)
      }
      .recover { case e: NumberFormatException => -10 }
      .into(Output.vector)
      .run

    assert(res.get == Vector(1, 2, 3, -10))
  }

  "DataPipeline.flatMap(getWords)" should "be a pipeline of words" in {
    val pipeline            = Input.sequential[Seq].create(Seq("Hello world", "hello you to"))
    val res: Vector[String] = pipeline.mapConcat(_.split("\\s")).into(Output.vector).run
    assert(res == Vector("Hello", "world", "hello", "you", "to"))
  }

  "DataPipeline.sorted" should "be sorted" in {
    val pipeline            = Input.sequential[Seq].create(Seq(5, 4, 3, 1))
    val sorted: Vector[Int] = pipeline.sorted.into(Output.vector).run
    assert(sorted == Vector(1, 3, 4, 5))
  }

  "DataPipeline.sortBy(_.length)" should "be sorted by length" in {
    val pipeline            = Input.sequential[Seq].create(Vector("a", "abcd", "bcd"))
    val res: Vector[String] = pipeline.sortBy(_.length).into(Output.vector).run
    assert(res == Vector("a", "bcd", "abcd"))
  }

  "Output.reduce(_+_)" should "produce pipeline sum" in {
    val pipeline = Input.sequential[Seq].create(Vector(1, 2, 3))
    val res: Int = pipeline.into(Output.reduce[Int](_ + _)).run
    assert(res == 6)
  }

  "Output.reduce" should "throw NoSuchElementException on empty pipeline" in {
    val pipeline = Input.sequential[Seq].empty[Int]
    assertThrows[UnsupportedOperationException] {
      val result: Int = pipeline.into(Output.reduce[Int](_ + _)).run
      result
    }
  }

  "Output.reduceOpt" should "work" in {
    val pipeline         = Input.sequential[Seq].create(Seq(1, 2, 3))
    val res: Option[Int] = pipeline.into(Output.reduceOpt[Int](_ + _)).run
    assert(res.contains(6))
  }

  "Output.reduceOpt" should "produce None on empty pipeline" in {
    val pipeline         = Input.sequential[Seq].empty[Int]
    val res: Option[Int] = pipeline.into(Output.reduceOpt[Int](_ + _)).run
    assert(res.isEmpty)
  }

  "Output.combineAll" should "work correctly" in {
    val pipeline = Input.sequential[Seq].create(Seq(1, 2, 3, 4, 5))
    val res: Int = pipeline.into(Output.combineAll[Int]).run
    assert(res == 15)
  }

  "Output.combineAll" should "produce empty result on empty pipeline" in {
    val pipeline = Input.sequential[Seq].empty[Int]
    val res: Int = pipeline.into(Output.combineAll[Int]).run
    assert(res == 0)
  }

  "Output.size" should "return pipeline size" in {
    val pipeline  = Input.sequential[Seq].create(Seq(1, 2, 3))
    val size: Int = pipeline.into(Output.size).run
    assert(size == 3)
  }

  "DataPipeline.groupBy" should "group elements" in {
    val pipeline = Input.sequential[Seq].create(Seq(1, 2, 3, 4))
    val grouped: Vector[(Boolean, List[Int])] = pipeline
      .groupByKey(_ % 2 == 0)
      .mapValues(_.toList)
      .sortBy(_._1)
      .into(Output.vector)
      .run

    assert(grouped == Vector(false -> List(1, 3), true -> List(2, 4)))
  }

  "DataPipeline.distinct" should "work" in {
    val pipeline              = Input.sequential[Seq].create(Seq(1, 2, 3, 1, 3, 2, 1))
    val distinct: Vector[Int] = pipeline.distinct.into(Output.vector).run
    assert(distinct.sorted == Vector(1, 2, 3))
  }

  "DataPipeline operations" should "be executed on each force" in {
    var x: Int = 0
    val pipeline = Input
      .sequential[Seq]
      .create(Seq(1, 2, 3))
      .map { i =>
        x += 1; i
      }
    val res1: Vector[Int] = pipeline.into(Output.vector).run
    assert(x == 3)
    val res2: Vector[Int] = pipeline.into(Output.vector).run
    assert(x == 6)
  }

  "split(n)" should "produce collection with n subcollections" in {
    val list    = List(1, 2, 3, 4, 5, 6, 7, 8)
    val grouped = BatchUtils.batch(4)(list).map(_.toList).toList
    assert(grouped == List(List(1, 2), List(3, 4), List(5, 6), List(7, 8)))
    val list2    = List(1, 2)
    val grouped2 = BatchUtils.batch(4)(list2).map(_.toList).toList
    assert(grouped2 == List(List(1, 2)))
    val list3    = Nil
    val grouped3 = BatchUtils.batch(2)(list3).map(_.toList).toList
    assert(grouped3 == Nil)
  }

  "PairPipeline transformations" should "work correctly" in {
    val pipeline = Input.sequential[Seq].create(Seq("a" -> 1, "b" -> 2, "c" -> 3))

    val result1: Vector[(String, Int)] = pipeline.mapValues(_ + 1).into(Output.vector).run
    assert(result1 == Vector("a" -> 2, "b" -> 3, "c" -> 4))

    val result2: Vector[String] = pipeline.keys.into(Output.vector).run
    assert(result2 == Vector("a", "b", "c"))

    val result3: Vector[Int] = pipeline.values.into(Output.vector).run
    assert(result3 == Vector(1, 2, 3))
  }

  "DataPipeline.zip" should "work correctly for pipelines" in {
    val p1                            = Input.sequential[Seq].create(Seq(1, 2, 3, 4))
    val p2                            = Input.sequential[Seq].create(Seq("a", "b", "c"))
    val result: Vector[(Int, String)] = p1.zip(p2).into(Output.vector).run
    assert(result == Vector(1 -> "a", 2 -> "b", 3 -> "c"))
  }

  "DataPipeline.++" should "work correctly for pipelines" in {
    val p1                  = Input.sequential[Seq].create(Seq(1, 2, 3, 4))
    val p2                  = Input.sequential[Seq].create(Seq(5, 6, 7))
    val result: Vector[Int] = (p1 ++ p2).sorted.into(Output.vector).run
    assert(result == Vector(1, 2, 3, 4, 5, 6, 7))
  }

  "DataPipeline.take" should "work correctly" in {
    val pipeline            = Input.sequential[Seq].create(Seq(1, 2, 3, 4))
    val result: Vector[Int] = pipeline.take(2).into(Output.vector).run
    assert(result == Vector(1, 2))
  }

  "DataPipeline.drop" should "work correctly" in {
    val pipeline            = Input.sequential[Seq].create(Seq(1, 2, 3, 4))
    val result: Vector[Int] = pipeline.drop(2).into(Output.vector).run
    assert(result == Vector(3, 4))
  }

  "DataPipeline.slice" should "work correctly" in {
    val pipeline            = Input.sequential[Seq].create(Seq(1, 2, 3, 4, 5))
    val result: Vector[Int] = pipeline.slice(1, 4).into(Output.vector).run
    assert(result == Vector(2, 3, 4))
  }

  "DataPipeline[IO]" should "produce the result wrapped in IO monad" in {
    val resultIO = Input
      .sequentialF[IO, Seq]
      .create[(String, Int)](
        IO(List("a" -> 1, "b" -> 2, "c" -> 3, "a" -> 3, "c" -> 10))
      )
      .groupByKey(_._1)
      .mapValues(_.foldLeft(0) { case (acc, (_, x)) => acc + x } * 10)
      .map { case (k, v) => s"{key=$k, value=$v}" }
      .sorted
      .into(Output.vector)
      .run
      .map(_.toList.mkString(", "))

    assert(
      resultIO.unsafeRunSync() == "{key=a, value=40}, {key=b, value=20}, {key=c, value=130}"
    )
  }

  "DataPipeline of IO" should "be paused correctly" in {
    val pipeline  = Input.sequentialF[IO, Seq].create(IO(Seq(1, 2, 3, 4, 5)))
    val paused    = pipeline.pausedWith(_.seconds)
    val startTime = System.currentTimeMillis()

    val evaled = paused.into(Output.vector).run.unsafeRunSync()

    val endTime = System.currentTimeMillis()
    assert(evaled == Vector(1, 2, 3, 4, 5))
    assert((endTime - startTime).millis >= evaled.sum.seconds)
  }

  "DataPipeline of IO" should "be paused correctly with CanPause2" in {
    val pipeline  = Input.sequentialF[IO, Seq].create(IO(Seq(1, 2, 3, 4, 5)))
    val paused    = pipeline.pausedWith2((a, b) => (b - a).seconds)
    val startTime = System.currentTimeMillis()

    val evaled = paused.into(Output.vector).run.unsafeRunSync()

    val endTime = System.currentTimeMillis()
    assert(evaled == Vector(1, 2, 3, 4, 5))
    assert((endTime - startTime).millis >= 4.seconds)
  }

  "DataPipeline transformations wrapped in Kleisli" should "be evaluated correctly" in {
    val pipeline = Input.sequentialF[IO, Seq].create(IO(Seq(1, 2, 3, 4, 5)))
    val pipe = pipeT[IO, Int, String, Sequential](
      _.mapM(i => IO { i + 1 })
        .filter(_ % 2 == 0)
        .map(_.toString)
    )

    val evaled = pipeline
      .through(pipe)
      .into(Output.vector)
      .run
      .unsafeRunSync()

    assert(evaled == Vector("2", "4", "6"))
  }
}
