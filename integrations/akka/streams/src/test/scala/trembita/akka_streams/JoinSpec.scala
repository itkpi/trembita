package trembita.akka_streams

import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import akka.stream.{ActorMaterializer, DelayOverflowStrategy}
import akka.testkit.TestKit
import cats.effect.{IO, Timer}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}
import trembita._
import scala.concurrent.ExecutionContext

class JoinSpec extends TestKit(ActorSystem("trembita-akka-join")) with FlatSpecLike with BeforeAndAfterAll {
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

  "Akka pipelines" should "support join" in {
    val ppln1 = Input.fromSourceF[IO](Source(1 to 10))
    val ppln2 = Input.fromSourceF[IO](Source(0 :: 1 :: Nil))

    val result = ppln1.join(ppln2)(on = _ % 2 == _).into(Output.vector).run.unsafeRunSync().sortBy(_._1)

    assert(result == (1 to 10).toVector.map(x => x -> (x % 2)))
  }

  it should "support join left" in {
    val ppln1 = Input.fromSourceF[IO](Source(1 to 10))
    val ppln2 = Input.fromSourceF[IO](Source(0 :: 1 :: Nil))

    val result = ppln1.joinLeft(ppln2)(on = _ % 3 == _).into(Output.vector).run.unsafeRunSync().sortBy(_._1)

    assert(result == (1 to 10).toVector.map { x =>
      x -> (x % 3 match {
        case 0 => Some(0)
        case 1 => Some(1)
        case _ => None
      })
    })
  }

  it should "support join right" in {
    val ppln1 = Input.fromSourceF[IO](Source(1 to 10))
    val ppln2 = Input.fromSourceF[IO](Source(0 :: 1 :: Nil))

    val result = ppln2.joinRight(ppln1)(on = _ == _ % 3).into(Output.vector).run.unsafeRunSync().sortBy(_._2)

    assert(
      result == (1 to 10).toVector
        .map { x =>
          x -> (x % 3 match {
            case 0 => Some(0)
            case 1 => Some(1)
            case _ => None
          })
        }
        .map(_.swap)
    )
  }
}
