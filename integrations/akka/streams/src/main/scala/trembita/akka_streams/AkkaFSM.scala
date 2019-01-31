package trembita.akka_streams

import akka.stream._
import akka.stream.scaladsl._
import akka.stream.stage._
import trembita.fsm.{CanFSM, FSM, InitialState}
import trembita._
import cats.arrow.FunctionK
import cats.effect.IO
import cats._
import cats.instances.either._
import cats.instances.try_._
import scala.annotation.implicitNotFound
import scala.concurrent.{ExecutionContext, Future}
import scala.language.higherKinds
import scala.util.Try

@implicitNotFound("""
    Unable to run Finite state machine in context ${F}
    upon stream with materialized value ${Mat}.
    Please try to define implicit ExecutionContext in the scope
  """)
trait AkkaFSM[F[_], Mat] {
  def apply[A, N, D, B](source: Source[A, Mat])(initial: InitialState[N, D, F])(
      fsmF: FSM.Empty[F, N, D, A, B] => FSM.Func[F, N, D, A, B]
  ): Source[B, Mat]
}

object AkkaFSM {
  implicit def idFsm[Mat](implicit ec: ExecutionContext): AkkaFSM[Id, Mat] =
    new AkkaFSM[Id, Mat] {
      def apply[A, N, D, B](
          source: Source[A, Mat]
      )(initial: InitialState[N, D, Id])(
          fsmF: FSM.Empty[Id, N, D, A, B] => FSM.Func[Id, N, D, A, B]
      ): Source[B, Mat] = {
        val stage =
          new FSMGraphF[Id, A, N, D, B](initial, fsmF, toFuture = idTo[Future])
        source.via(stage)
      }
    }

  implicit def tryFsm[Mat](implicit ec: ExecutionContext): AkkaFSM[Try, Mat] =
    new AkkaFSM[Try, Mat] {
      def apply[A, N, D, B](
          source: Source[A, Mat]
      )(initial: InitialState[N, D, Try])(
          fsmF: FSM.Empty[Try, N, D, A, B] => FSM.Func[Try, N, D, A, B]
      ): Source[B, Mat] = {
        val stage =
          new FSMGraphF[Try, A, N, D, B](initial, fsmF, toFuture = tryToFuture)
        source.via(stage)
      }
    }

  implicit def eitherFSM[Mat](
      implicit ec: ExecutionContext
  ): AkkaFSM[Either[Throwable, ?], Mat] =
    new AkkaFSM[Either[Throwable, ?], Mat] {
      def apply[A, N, D, B](source: Source[A, Mat])(
          initial: InitialState[N, D, Either[Throwable, ?]]
      )(fsmF: FSM.Empty[Either[Throwable, ?], N, D, A, B] => FSM.Func[Either[
          Throwable,
          ?
        ], N, D, A, B]): Source[B, Mat] = {
        val stage = new FSMGraphF[Either[Throwable, ?], A, N, D, B](
          initial,
          fsmF,
          toFuture = eitherToFuture
        )
        source.via(stage)
      }
    }

  implicit def futureFsm[Mat](
      implicit ec: ExecutionContext
  ): AkkaFSM[Future, Mat] = new AkkaFSM[Future, Mat] {
    def apply[A, N, D, B](
        source: Source[A, Mat]
    )(initial: InitialState[N, D, Future])(
        fsmF: FSM.Empty[Future, N, D, A, B] => FSM.Func[Future, N, D, A, B]
    ): Source[B, Mat] = {
      val stage =
        new FSMGraphF[Future, A, N, D, B](
          initial,
          fsmF,
          toFuture = FunctionK.id[Future]
        )
      source.via(stage)
    }
  }

  implicit def ioFsm[Mat](implicit ec: ExecutionContext): AkkaFSM[IO, Mat] =
    new AkkaFSM[IO, Mat] {
      def apply[A, N, D, B](
          source: Source[A, Mat]
      )(initial: InitialState[N, D, IO])(
          fsmF: FSM.Empty[IO, N, D, A, B] => FSM.Func[IO, N, D, A, B]
      ): Source[B, Mat] = {
        val stage =
          new FSMGraphF[IO, A, N, D, B](initial, fsmF, toFuture = ioToFuture)
        source.via(stage)
      }
    }
}

class FSMGraphF[F[_], A, N, D, B](
    initial: InitialState[N, D, F],
    fsmF: FSM.Empty[F, N, D, A, B] => FSM.Func[F, N, D, A, B],
    toFuture: F ~> Future
)(implicit applicative: Applicative[F], ec: ExecutionContext)
    extends GraphStage[FlowShape[A, B]] {
  val in: Inlet[A]   = Inlet[A]("FSM.in")
  val out: Outlet[B] = Outlet[B]("FSM.out")

  override val shape: FlowShape[A, B] = FlowShape.of(in, out)

  override def createLogic(attr: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) { self =>
      var stateOpt = Option.empty[FSM.State[N, D, F]]
      private val updateAndEmit =
        getAsyncCallback[(FSM.State[N, D, F], Iterable[B])] {
          case (newState, elems) ⇒
            stateOpt = Some(newState)
            emitMultiple(out, elems.iterator)
            if (!hasBeenPulled(in)) pull(in)
        }
      setHandler(
        in,
        new InHandler {
          val stateF: FSM.Func[F, N, D, A, B] =
            fsmF(new FSM.Empty[F, N, D, A, B])

          override def onPush(): Unit = {
            val a = grab(in)
            val currState = stateOpt match {
              case None =>
                initial match {
                  case InitialState.Pure(s) => s
                  case InitialState
                        .FromFirstElement(f: (A => FSM.State[N, D, Future]) @unchecked) =>
                    f(a)
                }
              case Some(s) => s
            }
            val xF      = stateF(currState)(a)
            val xFuture = toFuture(xF)
            xFuture.foreach(updateAndEmit.invoke)
          }
        }
      )
      setHandler(out, new OutHandler {
        override def onPull(): Unit =
          if (!hasBeenPulled(in)) pull(in)
      })
    }
}
