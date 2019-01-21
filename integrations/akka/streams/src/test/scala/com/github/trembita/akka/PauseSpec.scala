package com.github.trembita.akka

import akka.NotUsed
import cats.effect.{IO, Timer}
import com.github.trembita.DataPipelineT
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}

import scala.concurrent.duration._
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, DelayOverflowStrategy, KillSwitches}
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.testkit.TestKit

import scala.concurrent.ExecutionContext
import scala.reflect.ClassTag
import scala.util.Random

class PauseSpec extends TestKit(ActorSystem("trembita-akka-pause")) with FlatSpecLike with BeforeAndAfterAll {

  implicit val _system: ActorSystem                         = system
  implicit val mat: ActorMaterializer                       = ActorMaterializer()(system)
  implicit val parallelism: Parallelism                     = Parallelism(4, ordered = false)
  implicit val ec: ExecutionContext                         = system.dispatcher
  implicit val delayOverflowStrategy: DelayOverflowStrategy = DelayOverflowStrategy.dropHead
  implicit val ioTimer: Timer[IO]                           = IO.timer(ec)

  override def afterAll(): Unit = {
    mat.shutdown()
    system.terminate()
  }

  "DataPipeline of IO" should "be paused correctly" in {
    val pipeline  = DataPipelineT.fromRepr[IO, Int, Akka[NotUsed]](Source(1 to 5))
    val paused    = pipeline.pausedWith(_.seconds)
    val startTime = System.currentTimeMillis()
    val evaled    = paused.eval.flatMap(fa => IO.fromFuture(IO(fa))).unsafeRunSync()
    val endTime   = System.currentTimeMillis()
    assert(evaled == Vector(1, 2, 3, 4, 5))
    assert(((endTime - startTime).millis - 15.seconds) <= 1.second)
  }

  "DataPipeline of IO" should "be paused correctly with CanPause2" in {
    val pipeline  = DataPipelineT.fromRepr[IO, Int, Akka[NotUsed]](Source(1 to 5))
    val paused    = pipeline.pausedWith2((a, b) => (b - a).seconds)
    val startTime = System.currentTimeMillis()
    val evaled    = paused.eval.flatMap(fa => IO.fromFuture(IO(fa))).unsafeRunSync()
    val endTime   = System.currentTimeMillis()
    assert(evaled == Vector(1, 2, 3, 4, 5))
    assert(((endTime - startTime).millis - 4.seconds) <= 1.second)
  }

  "DataPipeline of IO" should "be paused correctly with CanPause2 on source with single element" in {
    val pipeline  = DataPipelineT.fromRepr[IO, Int, Akka[NotUsed]](Source.single(1))
    val paused    = pipeline.pausedWith2((a, b) => (b - a).seconds)
    val startTime = System.currentTimeMillis()
    val evaled    = paused.eval.flatMap(fa => IO.fromFuture(IO(fa))).unsafeRunSync()
    val endTime   = System.currentTimeMillis()
    assert(evaled == Vector(1))
    assert((endTime - startTime).millis <= 1.second)
  }

  "DataPipeline of IO" should "be paused correctly with CanPause2 on empty source" in {
    val pipeline  = DataPipelineT.fromRepr[IO, Int, Akka[NotUsed]](Source.empty)
    val paused    = pipeline.pausedWith2((a, b) => (b - a).seconds)
    val startTime = System.currentTimeMillis()
    val evaled    = paused.eval.flatMap(fa => IO.fromFuture(IO(fa))).unsafeRunSync()
    val endTime   = System.currentTimeMillis()
    assert(evaled == Vector())
    assert((endTime - startTime).millis <= 1.second)
  }

  "DataPipeline of IO" should "be paused correctly with CanPause2 on infinite graph" in {
    var acc: Int = 0
    val pipeline: DataPipelineT[IO, String, Akka[NotUsed]] = DataPipelineT
      .fromRepr[IO, Int, Akka[NotUsed]](Source.fromIterator(() => Iterator.from(1)))
      .pausedWith2((a, b) => (b - a).seconds)
      .map { i =>
        acc += 1
        s"After pause: $i"
      }

    val killSwitch = pipeline.evalRepr
      .map(
        _.viaMat(KillSwitches.single)(Keep.right)
          .to(Sink.foreach(println))
          .run()
      )
      .unsafeRunSync()
    Thread.sleep(6000)
    assert(acc == 6)
    killSwitch.shutdown()
  }
}
