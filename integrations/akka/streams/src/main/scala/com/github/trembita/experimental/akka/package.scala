package com.github.trembita.experimental

import _root_.akka.stream.Materializer
import _root_.akka.stream.scaladsl._
import _root_.akka.NotUsed
import cats.{~>, Monad}
import cats.effect.IO
import com.github.trembita.fsm.{CanFSM, FSM, InitialState}
import com.github.trembita.{DataPipelineT, Environment}
import com.github.trembita.operations.{LiftPipeline, MagnetlessOps}

import scala.concurrent.{ExecutionContext, Future}
import scala.language.higherKinds
import scala.language.implicitConversions
import scala.reflect.ClassTag

package object akka {
  type Akka = AkkaMat[NotUsed] with Environment.ReprAux[Source[?, NotUsed]]

  implicit class AkkaOps[F[_], A, Mat](
      val `this`: DataPipelineT[F, A, AkkaMat[Mat]]
  ) extends AnyVal
      with MagnetlessOps[F, A, AkkaMat[Mat]] {
    def evalWith(run: AkkaMat[Mat]#Run[F])(
        implicit e: AkkaMat[Mat],
        F: Monad[F],
        arrow: AkkaMat[Mat]#Result ~> F
    ): F[Vector[A]] =
      F.flatMap(`this`.eval(F, e, run.asInstanceOf[e.Run[F]]))(arrow(_))

    def runForeach(
        f: A => Unit
    )(implicit e: AkkaMat[Mat], run: AkkaMat[Mat]#Run[F], F: Monad[F], arrow: AkkaMat[Mat]#Result ~> F): F[Unit] =
      F.flatMap(`this`.foreach(f))(arrow(_))

    def runForeachF(
        f: A => F[Unit]
    )(implicit e: AkkaMat[Mat], run: AkkaMat[Mat]#Run[F], F: Monad[F], arrow: AkkaMat[Mat]#Result ~> F): F[Unit] =
      `this`.foreachF(f)
  }

  implicit def deriveAkka[Mat](
      implicit mat: Materializer,
      ec: ExecutionContext
  ): AkkaMat[Mat] with Environment.ReprAux[Source[?, Mat]] =
    AkkaMat.akka(ec, mat)

  implicit def liftToAkka[F[_]: Monad](
      implicit mat: Materializer,
      ec: ExecutionContext
  ): LiftPipeline[F, Akka] = new LiftAkkaPipeline[F]

  implicit def deriveRunFutureOnAkka(implicit parallelism: Parallelism, mat: Materializer): RunAkka[Future] =
    new RunFutureOnAkka(parallelism)

  implicit def deriveRunIOOnAkka(implicit parallelism: Parallelism, mat: Materializer): RunAkka[IO] =
    new RunIOOnAkka(parallelism)

  implicit def parallelism[F[_], E <: Environment](
      p: Parallelism
  )(implicit RunDsl: RunDsl[F]): RunAkka[F] =
    RunDsl.toRunAkka(p)

  implicit def akkaCanFSM[F[_], Mat](
      implicit akkaFSM: AkkaFSM[F, Mat],
      F: Monad[F],
      E: AkkaMat[Mat],
      run: AkkaMat[Mat]#Run[F]
  ): CanFSM[F, AkkaMat[Mat]] =
    new CanFSM[F, AkkaMat[Mat]] {
      def fsm[A: ClassTag, N, D, B: ClassTag](
          pipeline: DataPipelineT[F, A, AkkaMat[Mat]]
      )(initial: InitialState[N, D, F])(
          fsmF: FSM.Empty[F, N, D, A, B] => FSM.Func[F, N, D, A, B]
      ): DataPipelineT[F, B, AkkaMat[Mat]] =
        pipeline.mapRepr[B](akkaFSM[A, N, D, B](_)(initial)(fsmF))
    }
}
