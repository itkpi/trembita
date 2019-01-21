package com.github.trembita

import _root_.akka.stream.{ActorMaterializer, DelayOverflowStrategy, Materializer}
import _root_.akka.stream.scaladsl._
import _root_.akka.NotUsed
import cats.{~>, Monad}
import cats.effect.{Effect, IO, Timer}
import com.github.trembita.fsm.{CanFSM, FSM, InitialState}
import com.github.trembita.{DataPipelineT, Environment}
import com.github.trembita.operations._

import scala.collection.immutable.SortedMap
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.language.higherKinds
import scala.language.implicitConversions
import scala.reflect.ClassTag
import cats.syntax.all._
import com.github.trembita.util.LiftFuture

import scala.collection.mutable.ListBuffer

package object akka {
  implicit class AkkaOps[F[_], A, Mat](
      val `this`: DataPipelineT[F, A, Akka[Mat]]
  ) extends AnyVal
      with MagnetlessOps[F, A, Akka[Mat]] {

    def runForeach(
        f: A => Unit
    )(implicit e: Akka[Mat], run: Akka[Mat]#Run[F], F: Monad[F], arrow: Akka[Mat]#Result ~> F): F[Unit] =
      F.flatMap(`this`.foreach(f))(arrow(_))

    def runForeachF(
        f: A => F[Unit]
    )(implicit e: Akka[Mat], run: Akka[Mat]#Run[F], F: Monad[F], arrow: Akka[Mat]#Result ~> F): F[Unit] =
      `this`.foreachF(f)

    def runAndForget()(
        implicit e: Akka[Mat],
        run: Akka[Mat]#Run[F],
        F: Monad[F],
        mat: ActorMaterializer,
        liftFuture: LiftFuture[F]
    ): F[Unit] =
      `this`.evalRepr.flatMap { source =>
        liftFuture {
          source.runWith(Sink.ignore)
        }.void
      }
  }

  implicit def deriveAkka[Mat](
      implicit mat: Materializer,
      ec: ExecutionContext
  ): Akka[Mat] with Environment.ReprAux[Source[?, Mat]] =
    Akka.akka(ec, mat)

  implicit def liftToAkka[F[_]: Monad](
      implicit mat: Materializer,
      ec: ExecutionContext
  ): LiftPipeline[F, Akka[NotUsed]] = new LiftAkkaPipeline[F]

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
      E: Akka[Mat],
      run: Akka[Mat]#Run[F]
  ): CanFSM[F, Akka[Mat]] =
    new CanFSM[F, Akka[Mat]] {
      def fsm[A: ClassTag, N, D, B: ClassTag](
          pipeline: DataPipelineT[F, A, Akka[Mat]]
      )(initial: InitialState[N, D, F])(
          fsmF: FSM.Empty[F, N, D, A, B] => FSM.Func[F, N, D, A, B]
      ): DataPipelineT[F, B, Akka[Mat]] =
        pipeline.mapRepr[B](akkaFSM[A, N, D, B](_)(initial)(fsmF))
    }

  implicit def canFlatMapAkka[Mat]: CanFlatMap[Akka[Mat]] = new CanFlatMap[Akka[Mat]] {
    override def flatMap[A, B](fa: Source[A, Mat])(
        f: A => Source[B, Mat]
    ): Source[B, Mat] = fa.flatMapConcat(f)
  }

  implicit def canGroupByAkka[Mat]: CanGroupBy[Source[?, Mat]] = new CanGroupBy[Source[?, Mat]] {
    import com.github.trembita.collections._

    def groupBy[K: ClassTag, V: ClassTag](vs: Source[V, Mat])(f: V => K): Source[(K, Iterable[V]), Mat] = {
      val groupFlow: Flow[(K, V), (K, Iterable[V]), NotUsed] = Flow[(K, V)]
        .fold(Map.empty[K, Vector[V]]) {
          case (m, (k, v)) => m.modify(k, Vector(v))(_ :+ v)
        }
        .mapConcat { x =>
          x
        }

      vs.map(v => f(v) -> v).via(groupFlow)
    }
  }

  implicit def canSpanByAkka[Mat]: CanSpanBy[Source[?, Mat]] = new CanSpanBy[Source[?, Mat]] {
    override def spanBy[K: ClassTag, V: ClassTag](fa: Source[V, Mat])(
        f: V => K
    ): Source[(K, Iterable[V]), Mat] =
      fa.map(v => f(v) -> v)
        .via(new SpanByFlow[K, V, ListBuffer[V]](v => ListBuffer(v), _ :+ _))
        .map { case (k, vs) => (k, vs.toVector) }
  }

  implicit def canGroupByOrderedAkka[Mat]: CanGroupByOrdered[Source[?, Mat]] = new CanGroupByOrdered[Source[?, Mat]] {
    import com.github.trembita.collections._

    def groupBy[K: ClassTag: Ordering, V: ClassTag](vs: Source[V, Mat])(f: V => K): Source[(K, Iterable[V]), Mat] = {
      val groupFlow: Flow[(K, V), (K, Iterable[V]), NotUsed] = Flow[(K, V)]
        .fold(SortedMap.empty[K, Vector[V]]) {
          case (m, (k, v)) => m.modify(k, Vector(v))(_ :+ v)
        }
        .mapConcat { x =>
          x
        }

      vs.map(v => f(v) -> v).via(groupFlow)
    }
  }

  implicit def canZipAkka[Mat]: CanZip[Source[?, Mat]] = new CanZip[Source[?, Mat]] {
    def zip[A: ClassTag, B: ClassTag](fa: Source[A, Mat], fb: Source[B, Mat]): Source[(A, B), Mat] = fa zip fb
  }

  implicit def canPauseAkka[F[_]: Effect: Timer, Mat](
      implicit mat: ActorMaterializer,
      ec: ExecutionContext,
      runAkka: RunAkka[F]
  ): CanPause[F, Akka[Mat]] = new CanPauseAkkaF[F, Mat]

  implicit def canPause2Akka[F[_]: Effect: Timer, Mat](
      implicit mat: ActorMaterializer,
      ec: ExecutionContext,
      runAkka: RunAkka[F]
  ): CanPause2[F, Akka[Mat]] = new CanPause2AkkaF[F, Mat]

  implicit def akkaCanToVector[Mat](implicit mat: ActorMaterializer): CanToVector.Aux[Source[?, Mat], Future] =
    new CanToVector[Source[?, Mat]] {
      override type Result[X] = Future[X]
      override def apply[A](fa: Source[A, Mat]): Future[Vector[A]] = fa.runWith(Sink.collection[A, Vector[A]])
    }
}
