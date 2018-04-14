package com.datarootlabs


import scala.language.{higherKinds, implicitConversions}
import cats._
import cats.implicits._
import cats.arrow._
import cats.data.Kleisli
import cats.effect.{Effect, IO, Sync}
import com.datarootlabs.trembita.parallel._
import com.datarootlabs.trembita.internal._
import scala.collection.generic.CanBuildFrom
import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}


package object trembita {
  //  type PairPipeline[K, V] = DataPipeline[(K, V)]
  //  type ParPairPipeline[K, V] = ParDataPipeline[(K, V)]
  //
  //  /**
  //    * Operations for [[DataPipeline]] of tuples
  //    * (NOT [[MapPipeline]])
  //    **/
  //  implicit class PairPipelineOps[K, V](val self: PairPipeline[K, V]) extends AnyVal {
  //    def mapValues[W](f: V ⇒ W): PairPipeline[K, W] = self.map { case (k, v) ⇒ (k, f(v)) }
  //    def keys: DataPipeline[K] = self.map(_._1)
  //    def values: DataPipeline[V] = self.map(_._2)
  //
  //    /**
  //      * Merges all values [[V]]
  //      * with the same key [[K]]
  //      *
  //      * @param f - reducing function
  //      * @return - reduced pipeline
  //      **/
  //    def reduceByKey(f: (V, V) ⇒ V): PairPipeline[K, V] =
  //      self.groupBy(_._1).mapValues { vs ⇒
  //        vs.foldLeft(Option.empty[V]) {
  //          case (None, (_, v)) ⇒ Some(v)
  //          case (acc, (_, v))  ⇒ acc.map(f(_, v))
  //        }.get
  //      }
  //
  //    /**
  //      * Same to [[reduceByKey]] above
  //      * Merges all values [[V]]
  //      * having a [[Monoid]] defined
  //      *
  //      * @param vMonoid - Monoid for [[V]]
  //      * @return - reduced pipeline
  //      **/
  //    def reduceByKey(implicit vMonoid: Monoid[V]): PairPipeline[K, V] = reduceByKey((v1, v2) ⇒ vMonoid.combine(v1, v2))
  //
  //    /** @return - [[MapPipeline]] */
  //    def toMap: MapPipeline[K, V] = new BaseMapPipeline[K, V](self)
  //  }

  /**
    * Implementation of [[Monad]] for [[DataPipeline]]
    **/
  implicit def DataPipelineMonad[F[_], T <: PipelineType]
  (implicit F: MonadError[F, Throwable])
  : MonadError[DataPipeline[?, F, T], Throwable] =
    new MonadError[DataPipeline[?, F, T], Throwable] {
      def pure[A](x: A): DataPipeline[A, F, T] = DataPipeline.fromEffect[A, F, T]((List(x): Iterable[A]).pure[F])

      def flatMap[A, B](fa: DataPipeline[A, F, T])(f: A ⇒ DataPipeline[B, F, T]): DataPipeline[B, F, T] = fa.flatMap(f)

      def tailRecM[A, B](a: A)(f: A ⇒ DataPipeline[Either[A, B], F, T]): DataPipeline[B, F, T] = f(a).flatMap {
        case Left(xa) ⇒ tailRecM(xa)(f)
        case Right(b) ⇒ pure(b)
      }

      def raiseError[A](e: Throwable): DataPipeline[A, F, T] = DataPipeline.fromEffect(F.raiseError[Iterable[A]](e))
      override def handleError[A](fa: DataPipeline[A, F, T])(f: Throwable ⇒ A): DataPipeline[A, F, T] = fa.handleError(f)
      def handleErrorWith[A](fa: DataPipeline[A, F, T])(f: Throwable ⇒ DataPipeline[A, F, T]): DataPipeline[A, F, T] =
        fa.handleErrorWith(f)
    }
  implicit def DataPipelineMonoidK[F[_], T <: PipelineType]
  (implicit F: MonadError[F, Throwable]): MonoidK[DataPipeline[?, F, T]] = new MonoidK[DataPipeline[?, F, T]]{
    def empty[A]: DataPipeline[A, F, T] = new StrictSource[A, F, T](F.pure[Iterator[A]](Iterator.empty))
    def combineK[A](x: DataPipeline[A, F, T], y: DataPipeline[A, F, T]): DataPipeline[A, F, T] = ???
  }

  implicit object TraverseIterator extends Traverse[Iterator] {
    def traverse[G[_], A, B](fa: Iterator[A])(f: A ⇒ G[B])(implicit G: Applicative[G]): G[Iterator[B]] = {
      if (!fa.hasNext) G.pure[Iterator[B]](Iterator.empty)
      else Traverse[Vector].sequence(fa.map(f).toVector).map(_.toIterator)
    }

    def foldLeft[A, B](fa: Iterator[A], b: B)(f: (B, A) ⇒ B): B = fa.foldLeft(b)(f)
    def foldRight[A, B](fa: Iterator[A], lb: Eval[B])(f: (A, Eval[B]) ⇒ Eval[B]): Eval[B] = fa.foldRight(lb)(f)
  }
  implicit class PipelineOps[A, F[_], T <: PipelineType](val self: DataPipeline[A, F, T]) extends AnyVal with Ops[A, F, T]

  type Flow[A, B, F[_], T <: PipelineType] = Kleisli[DataPipeline[?, F, T], DataPipeline[A, F, T], B]

  def Flow[A, B, F[_], T <: PipelineType](run: DataPipeline[A, F, T] ⇒ DataPipeline[B, F, T]): Flow[A, B, F, T] = {
    Kleisli[DataPipeline[?, F, T], DataPipeline[A, F, T], B](run)
  }

  implicit def iterable2DataPipeline[A, F[_], T <: PipelineType]
  (iterable: Iterable[A])(implicit F: MonadError[F, Throwable])
  : DataPipeline[A, F, T] =
    DataPipeline.fromEffect[A, F, T](iterable.pure[F])

  implicit def array2DataPipeline[A, F[_], T <: PipelineType]
  (array: Array[A])(implicit F: MonadError[F, Throwable])
  : DataPipeline[A, F, T] =
    DataPipeline.fromEffect[A, F, T]((array: Iterable[A]).pure[F])

  implicit def TryT[G[_]](implicit G: MonadError[G, Throwable]): Try ~> G = {
    def tryToG[A](tryA: Try[A]): G[A] = tryA match {
      case Success(v) ⇒ G.pure(v)
      case Failure(e) ⇒ G.raiseError(e)
    }

    FunctionK.lift { tryToG }
  }

  implicit def IdT[F[_]]: F ~> F = FunctionK.id
  implicit val FutureToIO : Future ~> IO = {
    def futureToIo[A](fa: Future[A]): IO[A] = IO.fromFuture(IO.pure(fa))

    FunctionK.lift { futureToIo }
  }
  implicit val IO_ToFuture: IO ~> Future = {
    def ioToFuture[A](ioa: IO[A]): Future[A] = ioa.unsafeToFuture()

    FunctionK.lift { ioToFuture }
  }
}
