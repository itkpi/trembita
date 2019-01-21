package com.github.trembita.akka

import akka.stream._
import akka.stream.stage._
import scala.language.higherKinds
import cats.effect._
import com.github.trembita.DataPipelineT
import com.github.trembita.operations.{CanPause, CanPause2}
import cats.syntax.all._
import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.reflect.ClassTag

class CanPauseAkkaF[F[_]: Effect: Timer: RunAkka, Mat](implicit mat: ActorMaterializer, ec: ExecutionContext)
    extends CanPause[F, Akka[Mat]] {
  override def pausedWith[A: ClassTag](
      pipeline: DataPipelineT[F, A, Akka[Mat]]
  )(
      getPause: A => FiniteDuration
  ): DataPipelineT[F, A, Akka[Mat]] =
    pipeline.mapRepr(_.mapAsync(1) { a =>
      val pause = getPause(a)
      Effect[F]
        .toIO {
          Timer[F].sleep(pause).as(a)
        }
        .unsafeToFuture()
    }.async)
}

class CanPause2AkkaF[F[_]: Effect: Timer: RunAkka, Mat](implicit mat: ActorMaterializer, ec: ExecutionContext)
    extends CanPause2[F, Akka[Mat]] {
  override def pausedWith[A: ClassTag](
      pipeline: DataPipelineT[F, A, Akka[Mat]]
  )(
      getPause: (A, A) => FiniteDuration
  ): DataPipelineT[F, A, Akka[Mat]] = pipeline.mapRepr(_.via(new CanPause2AkkaFlow[A](getPause)).async)

  private class CanPause2AkkaFlow[A](getPause: (A, A) => FiniteDuration) extends GraphStage[FlowShape[A, A]] {
    val in                              = Inlet[A]("CanPause2.in")
    val out                             = Outlet[A]("CanPause2.out")
    override val shape: FlowShape[A, A] = FlowShape(in, out)

    var prevOpt: Option[A]         = None
    val inBuffer: mutable.Queue[A] = mutable.Queue.empty[A]
    var paused: Boolean            = false

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) with StageLogging {
      setHandlers(
        in,
        out,
        new InHandler with OutHandler {
          override def onPush(): Unit = {
            val curr = grab(in)
            log.debug(s"OnPush! curr=$curr, inBuffer=$inBuffer, prevOpt=$prevOpt, isPaused: $paused")
            if (prevOpt.isEmpty) {
              prevOpt = Some(curr)
              log.debug(s"Emitting first: $curr")
              push(out, curr)
            } else {
              inBuffer += curr
              scheduleIfNotPaused()
            }
          }

          override def onUpstreamFinish(): Unit = {
            log.debug(s"Upstream finished! inBuffer=$inBuffer, prevOpt=$prevOpt, isPaused: $paused")
            completeIfDone()
            scheduleIfNotPaused()
          }

          override def onPull(): Unit = {
            log.debug(s"OnPull! inBuffer=$inBuffer, prevOpt=$prevOpt, isPaused: $paused")
            myPull()
          }

          override def onDownstreamFinish(): Unit =
            log.debug(s"On downstream finish! inBuffer=$inBuffer, prevOpt=$prevOpt, isPaused: $paused")

          private def schedulePause(): Unit = {
            log.debug("SchedulePause")
            if (prevOpt.nonEmpty && inBuffer.nonEmpty) {
              log.debug("prevOpt is nonEmpty")
              paused = true
              val prev = prevOpt.get
              val curr = inBuffer.dequeue()
              prevOpt = Some(curr)
              val pauseDur = getPause(prev, curr)

              val asyncCallback: AsyncCallback[Unit] = getAsyncCallback[Unit] { _ =>
                log.debug(s"AsyncCallback invoked: inBuffer=$inBuffer, prevOpt=$prevOpt")
                paused = false
                log.debug(s"Emitting $curr")
                push(out, curr)
                log.debug(s"isClosed(in)=${isClosed(in)}")
                scheduleIfNotPaused()
              }
              log.debug(s"Sleeping with $pauseDur")
              val pauseF = Timer[F].sleep(pauseDur)
              Effect[F].toIO(pauseF).unsafeToFuture().foreach(asyncCallback.invoke)
            } else if (prevOpt.isEmpty && inBuffer.nonEmpty) {
              failStage(new RuntimeException(s"Impossible that prevOpt is $prevOpt and inBuffer is $inBuffer"))
            } else {
              log.debug(s"prevOpt is Empty, inBuffer is Empty")
              completeIfDone()
            }
          }

          private def scheduleIfNotPaused(): Unit =
            if (!paused) {
              schedulePause()
            }

          private def completeIfDone(): Unit = {
            log.debug(s"Complete if Done: inBuffer=$inBuffer, isClosed(in)=${isClosed(in)}, prevOpt=$prevOpt")
            if (isClosed(in) && !paused) {
              log.debug("Completing stage")
              completeStage()
            }
          }

          private def myPull(): Unit = {
            log.debug(s"Pulling... hasBeenPulled=${hasBeenPulled(in)}")
            pull(in)
          }
        }
      )
    }
  }
}
