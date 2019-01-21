//package com.github.trembita.spark.streaming
//
//import cats.Id
//import cats.effect._
//import cats.implicits._
//import com.github.trembita._
//import com.github.trembita.operations.LiftPipeline
//import com.github.trembita.spark._
//import org.apache.spark.SparkContext
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.streaming.StreamingContext
//import org.scalatest.{BeforeAndAfterAll, FlatSpec}
//
//import scala.concurrent.{Await, Future}
//import scala.concurrent.duration._
//
//class StreamingSpec extends FlatSpec with BeforeAndAfterAll {
//  @transient val spark: SparkSession            = SparkSession.builder().master("local[2]").appName("test").getOrCreate()
//  @transient implicit val ssc: StreamingContext = new StreamingContext(spark.sparkContext, org.apache.spark.streaming.Duration(1000))
//  implicit val asyncTimeout: AsyncTimeout       = AsyncTimeout(10.seconds)
//
//  private val liftSeq = LiftPipeline[Id, SparkStreaming]
//
//  override def afterAll(): Unit = spark.close()
//
//  implicit val contextShift: ContextShift[IO] = IO.contextShift(globalSafeEc)
//  implicit val ioTimer: Timer[IO]             = IO.timer(globalSafeEc)
//
//  "DataPipeline operations" should "not be executed until 'eval'" in {
//    val pipeline = liftSeq.liftIterable(Seq(1, 2, 3))
//    pipeline.map(_ => throw new Exception("Bang"))
//    assert(true)
//  }
//
//  "DataPipeline.map(square)" should "be mapped squared" in {
//    val pipeline        = liftSeq.liftIterable(Seq(1, 2, 3))
//    val res: Array[Int] = pipeline.map(i => i * i).into(Output.start[Int]).run
//    assert(res sameElements Array(1, 4, 9))
//  }
//
//  "DataPipeline.filter(isEven)" should "contain only even numbers" in {
//    val pipeline        = liftSeq.liftIterable(Seq(1, 2, 3))
//    val res: Array[Int] = pipeline.filter(_ % 2 == 0).into(Output.array[Int]).run
//    assert(res sameElements Array(2))
//  }
//
//  "DataPipeline.collect(toInt)" should "be DataPipeline[Int]" in {
//    val pipeline = liftSeq.liftIterable(Seq("1", "2", "3", "abc")))
//    val res: Array[Int] = pipeline
//      .collect {
//        case str if str.forall(_.isDigit) => str.toInt
//      }
//      .into(Output.array[Int])
//      .run
//    assert(res sameElements Array(1, 2, 3))
//  }
//
//  "DataPipeline[String, SerializableFutureImpl, ...].mapM(...toInt)" should "be DataPipeline[Int]" in {
//    val pipeline = Input.sequentialF[SerializableFuture, Vector].create(SerializableFuture.pure(Vector("1", "2", "3", "abc"))).to[Spark]
//    val res = pipeline
//      .mapM { str: String =>
//        SerializableFuture.start(str.toInt)
//      }
//      .recover { case e: NumberFormatException => -10 }
//      .into(Output.arrayF[SerializableFuture, Int])
//      .run
//
//    assert(Await.result(res, asyncTimeout.duration) sameElements Array(1, 2, 3, -10))
//  }
//
//  "DataPipeline.flatMap(getWords)" should "be a pipeline of words" in {
//    val pipeline           = liftSeq.liftIterable(Seq("Hello world", "hello you to"))
//    val res: Array[String] = pipeline.mapConcat(_.split("\\s")).into(Output.array[String]).run
//    assert(res sameElements Array("Hello", "world", "hello", "you", "to"))
//  }
//
//  "DataPipeline.sorted" should "be sorted" in {
//    val pipeline           = liftSeq.liftIterable(Seq(5, 4, 3, 1))
//    val sorted: Array[Int] = pipeline.sorted.into(Output.array[Int]).run
//    assert(sorted sameElements Array(1, 3, 4, 5))
//  }
//
//  "DataPipeline.sortBy(_.length)" should "be sorted by length" in {
//    val pipeline           = liftSeq.liftIterable(Vector("a", "abcd", "bcd"))
//    val res: Array[String] = pipeline.sortBy(_.length).into(Output.array[String]).run
//    assert(res sameElements Array("a", "bcd", "abcd"))
//  }
//
//  "DataPipeline.groupBy" should "group elements" in {
//    val pipeline = liftSeq.liftIterable(Seq(1, 2, 3, 4))
//    val grouped: Array[(Boolean, List[Int])] = pipeline
//      .groupBy(_ % 2 == 0)
//      .mapValues(_.toList)
//      .sortBy(_._1)
//      .into(Output.array[(Boolean, List[Int])])
//      .run
//
//    assert(grouped sameElements Array(false -> List(1, 3), true -> List(2, 4)))
//  }
//
//  "DataPipeline.distinct" should "work" in {
//    val pipeline             = liftSeq.liftIterable(Seq(1, 2, 3, 1, 3, 2, 1))
//    val distinct: Array[Int] = pipeline.distinct.into(Output.array[Int]).run
//    assert(distinct.sorted sameElements Array(1, 2, 3))
//  }
//
//  "PairPipeline transformations" should "work correctly" in {
//    val pipeline = liftSeq.liftIterable(Seq("a" -> 1, "b" -> 2, "c" -> 3))
//
//    val result1: Array[(String, Int)] = pipeline.mapValues(_ + 1).into(Output.array[(String, Int)]).run
//    assert(result1 sameElements Array("a" -> 2, "b" -> 3, "c" -> 4))
//
//    val result2: Array[String] = pipeline.keys.into(Output.array[String]).run
//    assert(result2 sameElements Array("a", "b", "c"))
//
//    val result3: Array[Int] = pipeline.values.into(Output.array[Int]).run
//    assert(result3 sameElements Array(1, 2, 3))
//  }
//
//  "DataPipeline.zip" should "work correctly for pipelines" in {
//    val p1                           = liftSeq.liftIterable(Seq(1, 2, 3))
//    val p2                           = liftSeq.liftIterable(Seq("a", "b", "c")))
//    val result: Array[(Int, String)] = p1.zip(p2).into(Output.array[(Int, String)]).run
//    assert(result sameElements Array(1 -> "a", 2 -> "b", 3 -> "c"))
//  }
//
//  "DataPipeline.++" should "work correctly for pipelines" in {
//    val p1                 = liftSeq.liftIterable(Seq(1, 2, 3, 4))
//    val p2                 = liftSeq.liftIterable(Seq(5, 6, 7))
//    val result: Array[Int] = (p1 ++ p2).sorted.into(Output.array[Int]).run
//    assert(result.toSeq == Vector(1, 2, 3, 4, 5, 6, 7))
//  }
//
//  "DataPipeline[IO]" should "produce the result wrapped in IO monad" in {
//    val resultIO = Input
//      .sequentialF[IO, Seq]
//      .create[(String, Int)](
//        IO(List("a" -> 1, "b" -> 2, "c" -> 3, "a" -> 3, "c" -> 10))
//      )
//      .to[Spark]
//      .groupBy(_._1)
//      .mapValues(_.foldLeft(0) { case (acc, (_, x)) => acc + x } * 10)
//      .map { case (k, v) => s"{key=$k, value=$v}" }
//      .sorted
//      .into(Output.arrayF[IO, String])
//      .run
//      .map(_.toList.mkString(", "))
//
//    assert(
//      resultIO.unsafeRunSync() == "{key=a, value=40}, {key=b, value=20}, {key=c, value=130}"
//    )
//  }
//}
