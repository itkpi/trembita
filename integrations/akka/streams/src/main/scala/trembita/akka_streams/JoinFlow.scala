package trembita.akka_streams

import akka.NotUsed
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.scaladsl._
import akka.stream.stage._

import scala.language.higherKinds

class JoinFlow[A, B](right: Stream[B], on: (A, B) => Boolean) extends GraphStage[FlowShape[A, (A, B)]] {
  def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogicWithLogging(shape) {
    setHandlers(
      in,
      out,
      new InHandler with OutHandler {
        def onPush(): Unit = {
          val curr = grab(in)
          log.debug(s"grabed $curr\n\tright=$right")
          right.find(on(curr, _)) match {
            case None =>
              log.debug(s"Haven't found pair for $curr")
            case Some(found) =>
              push(out, (curr, found))
              log.debug(s"Pushed pair: $curr -> $found")
          }
        }
        def onPull(): Unit = {
          log.debug("On pull...")
          pull(in)
        }
      }
    )
  }
  private val in                  = Inlet[A]("join.in")
  private val out                 = Outlet[(A, B)]("join.out")
  val shape: FlowShape[A, (A, B)] = FlowShape(in, out)
}

class JoinLeftFlow[A, B](right: Stream[B], on: (A, B) => Boolean) extends GraphStage[FlowShape[A, (A, Option[B])]] {
  def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogicWithLogging(shape) {
    setHandlers(
      in,
      out,
      new InHandler with OutHandler {
        def onPush(): Unit = {
          val curr = grab(in)
          log.debug(s"grabed $curr")
          right.find(on(curr, _)) match {
            case None =>
              log.debug(s"Haven't found pair for $curr")
              push(out, (curr, None))
            case res @ Some(found) =>
              push(out, (curr, res))
              log.debug(s"Pushed pair: $curr -> $found")
          }
        }
        def onPull(): Unit = {
          log.debug("Pulling...")
          pull(in)
        }
      }
    )
  }
  private val in                          = Inlet[A]("joinLeft.in")
  private val out                         = Outlet[(A, Option[B])]("joinLeft.out")
  val shape: FlowShape[A, (A, Option[B])] = FlowShape(in, out)
}
