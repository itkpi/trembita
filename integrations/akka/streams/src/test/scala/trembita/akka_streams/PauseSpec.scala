package trembita.akka_streams

import java.util.concurrent.atomic.AtomicInteger
import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import akka.stream.{ActorMaterializer, DelayOverflowStrategy, KillSwitches}
import akka.testkit.TestKit
import cats.effect.{IO, Timer}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}
import trembita._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

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
    val pipeline  = Input.fromSourceF[IO](Source(1 to 5))
    val paused    = pipeline.pausedWith(_.seconds)
    val startTime = System.currentTimeMillis()
    val evaled =
      paused
        .into(Output.vector)
        .run
        .unsafeRunSync()

    val endTime = System.currentTimeMillis()
    assert(evaled == Vector(1, 2, 3, 4, 5))
    assert(((endTime - startTime).millis - 15.seconds) <= 2.second)
  }

  "DataPipeline of IO" should "be paused correctly with CanPause2" in {
    val pipeline  = Input.fromSourceF[IO](Source(1 to 5))
    val paused    = pipeline.pausedWith2((a, b) => (b - a).seconds)
    val startTime = System.currentTimeMillis()
    val evaled = paused
      .into(Output.vector)
      .run
      .unsafeRunSync()
    val endTime = System.currentTimeMillis()
    assert(evaled == Vector(1, 2, 3, 4, 5))
    assert(((endTime - startTime).millis - 4.seconds) <= 2.second)
  }

  "DataPipeline of IO" should "be paused correctly with CanPause2 on source with single element" in {
    val pipeline  = Input.fromSourceF[IO](Source.single(1))
    val paused    = pipeline.pausedWith2((a, b) => (b - a).seconds)
    val startTime = System.currentTimeMillis()
    val evaled = paused
      .into(Output.vector)
      .run
      .unsafeRunSync()
    val endTime = System.currentTimeMillis()
    assert(evaled == Vector(1))
    assert((endTime - startTime).millis <= 1.second)
  }

  "DataPipeline of IO" should "be paused correctly with CanPause2 on empty source" in {
    val pipeline  = Input.fromSourceF[IO].empty[Int]
    val paused    = pipeline.pausedWith2((a, b) => (b - a).seconds)
    val startTime = System.currentTimeMillis()
    val evaled = paused
      .into(Output.vector)
      .run
      .unsafeRunSync()
    val endTime = System.currentTimeMillis()
    assert(evaled == Vector())
    assert((endTime - startTime).millis <= 2.second)
  }

  "DataPipeline of IO" should "be paused correctly with CanPause2 on infinite graph" in {
    val acc = new AtomicInteger()
    val pipeline: BiDataPipelineT[IO, String, Akka[NotUsed]] = Input
      .fromSourceF[IO](Source.fromIterator(() => Iterator.from(1)))
      .pausedWith2((a, b) => (b - a).seconds)
      .map { i =>
        acc.incrementAndGet()
        s"After pause: $i"
      }

    val killSwitch = pipeline
      .through(KillSwitches.single)
      .into(Output.ignore[String])
      .keepMat
      .run
      .unsafeRunSync()

    Thread.sleep(6000)
    assert(acc.get() == 6)
    killSwitch.shutdown()
  }
}
