package trembita.akka_streams

import akka.NotUsed
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import cats.{Monad, MonadError}
import cats.effect._
import trembita._
import trembita.collections._
import trembita.fsm.{CanFSM, FSM, InitialState}
import trembita.operations._
import trembita.outputs.internal.{collectionDsl, CollectionOutput, OutputWithPropsT}
import cats.instances.future._

import scala.collection.generic.CanBuildFrom
import scala.collection.immutable.SortedMap
import scala.collection.immutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.language.{higherKinds, implicitConversions}
import scala.reflect.ClassTag

trait operations {

  implicit def canReduceStreamByKey[Mat]: CanReduceByKey[Source[?, Mat]] = new CanReduceByKey[Source[?, Mat]] {
    def reduceByKey[K: ClassTag, V: ClassTag](fa: Source[(K, V), Mat])(
        reduce: (V, V) => V
    ): Source[(K, V), Mat] = {
      val reduceByKeyFlow: Flow[(K, V), (K, V), NotUsed] = Flow[(K, V)]
        .fold(Map.empty[K, V]) {
          case (m, (k, v)) if m contains k => m.updated(k, reduce(m(k), v))
          case (m, (k, v))                 => m + (k -> v)
        }
        .mapConcat { x =>
          x
        }

      fa.via(reduceByKeyFlow)
    }
  }

  implicit def canCombineStreamByKey[Mat]: CanCombineByKey[Source[?, Mat]] = new CanCombineByKey[Source[?, Mat]] {
    def combineByKey[K: ClassTag, V: ClassTag, C: ClassTag](
        fa: Source[(K, V), Mat]
    )(init: V => C, addValue: (C, V) => C, mergeCombiners: (C, C) => C): Source[(K, C), Mat] = {
      val groupFlow: Flow[(K, V), (K, C), NotUsed] =
        Flow[(K, V)]
          .fold(Map.empty[K, C]) {
            case (m, (k, v)) => m.modify(k, init(v))(addValue(_, v))
          }
          .mapConcat(x => x)

      fa.via(groupFlow)
    }
  }

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
    import trembita.collections._

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

    def distinctBy[A: ClassTag, B: ClassTag](fa: Source[A, Mat])(
        f: A => B
    ): Source[A, Mat] = {
      val distinctByFlow: Flow[A, A, NotUsed] = Flow[A]
        .fold((Set.empty[B], Vector.empty[A])) {
          case (acc0 @ (seen, values), a) =>
            val b = f(a)
            if (seen(b)) acc0
            else (seen + b, values :+ a)
        }
        .mapConcat(_._2)

      fa.via(distinctByFlow)
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
    import trembita.collections._

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

  implicit def canFoldAkka[Mat](implicit mat: ActorMaterializer): CanFold.Aux[Source[?, Mat], Future] =
    new CanFold[Source[?, Mat]] {
      type Result[x] = Future[x]
      def foldLeft[A: ClassTag, B: ClassTag](fa: Source[A, Mat])(zero: B)(
          f: (B, A) => B
      ): Future[B] = fa.runFold(zero)(f)

      def reduceOpt[A: ClassTag](fa: Source[A, Mat])(
          f: (A, A) => A
      ): Future[Option[A]] = foldLeft(fa)(zero = Option.empty[A]) {
        case (Some(acc), a) => Some(f(acc, a))
        case (None, a)      => Some(a)
      }

      def reduce[A: ClassTag](fa: Source[A, Mat])(f: (A, A) => A): Future[A] = fa.runReduce(f)
    }

  implicit def canDropAkka[Mat]: CanDrop[Source[?, Mat]] = new CanDrop[Source[?, Mat]] {
    def drop[A](fa: Source[A, Mat], n: Int): Source[A, Mat] = fa.drop(n)
  }

  implicit def canTakeAkka[Mat]: CanTake[Source[?, Mat]] = new CanTake[Source[?, Mat]] {
    def take[A](fa: Source[A, Mat], n: Int): Source[A, Mat] = fa.take(n)
  }

  implicit def dslAkkaOutput[F[_]: Async, Mat, Col[x] <: Iterable[x]](
      dsl: collectionDsl[Col]
  )(
      implicit mat: ActorMaterializer
  ): OutputWithPropsT.Aux[F, Akka[Mat], λ[A => CanBuildFrom[Col[A], A, Col[A]]], λ[(G[_], A) => G[Col[A]]]] =
    new AkkaCollectionOutput[Col, F, Mat]

  implicit def futureAsync(implicit ec: ExecutionContext): Async[Future] = new Async[Future] {
    private val monadError = catsStdInstancesForFuture(ec)
    def async[A](k: (Either[Throwable, A] => Unit) => Unit): Future[A] =
      IO.async[A](k).unsafeToFuture()

    def asyncF[A](
        k: (Either[Throwable, A] => Unit) => Future[Unit]
    ): Future[A] =
      IO.asyncF[A](cb => IO.fromFuture(IO(k(cb)))).unsafeToFuture()

    def suspend[A](thunk: => Future[A]): Future[A] = Future(thunk).flatMap(identity)

    def bracketCase[A, B](acquire: Future[A])(use: A => Future[B])(
        release: (A, ExitCase[Throwable]) => Future[Unit]
    ): Future[B] =
      IO.fromFuture(IO(acquire))
        .bracketCase(a => IO.fromFuture(IO(use(a))))((a, exitCase) => IO.fromFuture(IO(release(a, exitCase))))
        .unsafeToFuture()

    def raiseError[A](e: Throwable): Future[A] = monadError.raiseError(e)

    def handleErrorWith[A](fa: Future[A])(
        f: Throwable => Future[A]
    ): Future[A] = monadError.handleErrorWith(fa)(f)

    override def handleError[A](fa: Future[A])(f: Throwable => A): Future[A] = monadError.handleError(fa)(f)

    def pure[A](x: A): Future[A]                                      = monadError.pure(x)
    def flatMap[A, B](fa: Future[A])(f: A => Future[B]): Future[B]    = monadError.flatMap(fa)(f)
    override def map[A, B](fa: Future[A])(f: A => B): Future[B]       = monadError.map(fa)(f)
    def tailRecM[A, B](a: A)(f: A => Future[Either[A, B]]): Future[B] = monadError.tailRecM(a)(f)
  }

  implicit def sourceCanBeGrouped[Mat]: CanGrouped[Source[?, Mat]] = new CanGrouped[Source[?, Mat]] {
    def grouped[A](fa: Source[A, Mat], n: Int): Source[Iterable[A], Mat] = fa.grouped(n)
  }

  implicit def sourceCanBeBatcher[Mat]: CanBatched[Source[?, Mat]] = new CanBatched[Source[?, Mat]] {
    def batched[A](fa: Source[A, Mat], parts: Int): Source[Iterable[A], Mat] =
      fa.sliding(parts)
  }

  implicit def akkaCanFoldFuture[Mat](implicit mat: ActorMaterializer): CanFoldF[Source[?, Mat], Future] =
    new CanFoldF[Source[?, Mat], Future] {
      def foldF[A, B](fa: Source[A, Mat])(zero: B)(
          f: (B, A) => Future[B]
      ): Future[B] = fa.runWith(Sink.foldAsync(zero)(f))
    }

  implicit def akkaCanFoldEffect[F[_]: Effect, Mat](implicit mat: ActorMaterializer): CanFoldF[Source[?, Mat], F] =
    new CanFoldF[Source[?, Mat], F] {
      def foldF[A, B](fa: Source[A, Mat])(zero: B)(
          f: (B, A) => F[B]
      ): F[B] =
        Effect[F].liftIO(IO.fromFuture(IO {
          fa.runWith(Sink.foldAsync(zero)((b, a) => Effect[F].toIO(f(b, a)).unsafeToFuture()))
        }))
    }
}

class AkkaCollectionOutput[Col[x] <: Iterable[x], F[_], Mat](implicit async: Async[F], mat: ActorMaterializer)
    extends CollectionOutput[Col, F, Akka[Mat]] {
  protected def intoCollection[A: ClassTag](
      repr: Source[A, Mat]
  )(implicit F: Monad[F], cbf: CanBuildFrom[Col[A], A, Col[A]]): F[Col[A]] = async.liftIO {
    IO.fromFuture(IO {
      repr.runWith(Sink.collection[A, Col[A]](cbf.asInstanceOf[CanBuildFrom[Nothing, A, Col[A] with immutable.Traversable[_]]]))
    })
  }
}
