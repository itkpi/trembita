package com.github.trembita.experimental

import _root_.akka.stream.Materializer
import _root_.akka.stream.scaladsl._
import _root_.akka.NotUsed
import cats.{~>, Monad}
import cats.effect.IO
import com.github.trembita.fsm.{CanFSM, FSM, InitialState}
import com.github.trembita.{DataPipelineT, Environment}
import com.github.trembita.operations._

import scala.collection.immutable.SortedMap
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

  implicit def canFlatMapAkka[Mat]: CanFlatMap[AkkaMat[Mat]] = new CanFlatMap[AkkaMat[Mat]] {
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
}
