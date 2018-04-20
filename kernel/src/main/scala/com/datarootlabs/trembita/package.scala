package com.datarootlabs


import scala.language.{higherKinds, implicitConversions}
import cats._
import cats.implicits._
import cats.arrow._
import cats.data.Kleisli
import cats.effect.{Effect, IO, Sync}
import com.datarootlabs.trembita.internal._
import scala.collection.generic.CanBuildFrom
import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}


package object trembita {
  type PairPipeline[K, V, F[_], T <: Finiteness, Ex <: Execution] = DataPipeline[(K, V), F, T, Ex]

  /**
    * Operations for [[DataPipeline]] of tuples
    * (NOT [[MapPipeline]])
    **/
  implicit class PairPipelineOps[K, V, F[_], T <: Finiteness, Ex <: Execution](val self: PairPipeline[K, V, F, T, Ex]) extends AnyVal {
    def mapValues[W](f: V ⇒ W): PairPipeline[K, W, F, T, Ex] = self.map { case (k, v) ⇒ (k, f(v)) }

    def keys: DataPipeline[K, F, T, Ex] = self.map(_._1)

    def values: DataPipeline[V, F, T, Ex] = self.map(_._2)
  }

  implicit class FinitePairPipelineOps[K, V, F[_], Ex <: Execution](val self: PairPipeline[K, V, F, Finiteness.Finite, Ex]) extends AnyVal {
    /**
      * Merges all values [[V]]
      * with the same key [[K]]
      *
      * @param f - reducing function
      * @return - reduced pipeline
      **/
    def reduceByKey(f: (V, V) ⇒ V)(implicit ex: Ex, me: MonadError[F, Throwable]): PairPipeline[K, V, F, Finiteness.Finite, Ex] =
      self.groupBy(_._1).mapValues { vs ⇒
        vs.foldLeft(Option.empty[V]) {
          case (None, (_, v)) ⇒ Some(v)
          case (acc, (_, v))  ⇒ acc.map(f(_, v))
        }.get
      }


    /**
      * Same to [[reduceByKey]] above
      * Merges all values [[V]]
      * having a [[Monoid]] defined
      *
      * @param vMonoid - Monoid for [[V]]
      * @return - reduced pipeline
      **/
    def reduceByKey(implicit vMonoid: Monoid[V], ex: Ex, me: MonadError[F, Throwable]): PairPipeline[K, V, F, Finiteness.Finite, Ex] =
      reduceByKey((v1, v2) ⇒ vMonoid.combine(v1, v2))

    /** @return - [[MapPipeline]] */
    def toMap(implicit ex: Ex, me: MonadError[F, Throwable]): MapPipeline[K, V, F, Ex] =
      new BaseMapPipeline[K, V, F, Ex](self.asInstanceOf[DataPipeline[(K, V), F, Finiteness.Finite, Ex]], ex)
  }

  /**
    * Implementation of [[Monad]] for [[DataPipeline]]
    **/
  implicit def DataPipelineMonad[F[_], T <: Finiteness, Ex <: Execution]
  (implicit F: MonadError[F, Throwable])
  : MonadError[DataPipeline[?, F, T, Ex], Throwable] =
    new MonadError[DataPipeline[?, F, T, Ex], Throwable] {
      def pure[A](x: A): DataPipeline[A, F, T, Ex] = DataPipeline.fromEffect[A, F, T, Ex]((List(x): Iterable[A]).pure[F])

      def flatMap[A, B](fa: DataPipeline[A, F, T, Ex])(f: A ⇒ DataPipeline[B, F, T, Ex]): DataPipeline[B, F, T, Ex] = fa.flatMap(f)

      def tailRecM[A, B](a: A)(f: A ⇒ DataPipeline[Either[A, B], F, T, Ex]): DataPipeline[B, F, T, Ex] = f(a).flatMap {
        case Left(xa) ⇒ tailRecM(xa)(f)
        case Right(b) ⇒ pure(b)
      }

      def raiseError[A](e: Throwable): DataPipeline[A, F, T, Ex] = DataPipeline.fromEffect(F.raiseError[Iterable[A]](e))

      override def handleError[A](fa: DataPipeline[A, F, T, Ex])(f: Throwable ⇒ A): DataPipeline[A, F, T, Ex] = fa.handleError(f)

      def handleErrorWith[A](fa: DataPipeline[A, F, T, Ex])(f: Throwable ⇒ DataPipeline[A, F, T, Ex]): DataPipeline[A, F, T, Ex] =
        fa.handleErrorWith(f)
    }

  implicit class PipelineOps[A, F[_], T <: Finiteness, Ex <: Execution](val self: DataPipeline[A, F, T, Ex])
    extends AnyVal with Ops[A, F, T, Ex]

  implicit class FinitePipelineOps[A, F[_], Ex <: Execution](val self: DataPipeline[A, F, Finiteness.Finite, Ex])
    extends AnyVal with FiniteOps[A, F, Ex]

  implicit def iterable2DataPipeline[A, F[_], T <: Finiteness, Ex <: Execution]
  (iterable: Iterable[A])(implicit F: MonadError[F, Throwable])
  : DataPipeline[A, F, T, Ex] =
    DataPipeline.fromEffect[A, F, T, Ex](iterable.pure[F])

  implicit def array2DataPipeline[A, F[_], T <: Finiteness, Ex <: Execution]
  (array: Array[A])(implicit F: MonadError[F, Throwable])
  : DataPipeline[A, F, T, Ex] =
    DataPipeline.fromEffect[A, F, T, Ex]((array: Iterable[A]).pure[F])

  implicit def TryT[G[_]](implicit G: MonadError[G, Throwable]): Try ~> G = {
    def tryToG[A](tryA: Try[A]): G[A] = tryA match {
      case Success(v) ⇒ G.pure(v)
      case Failure(e) ⇒ G.raiseError(e)
    }

    FunctionK.lift {tryToG}
  }

  implicit def IdT[F[_]]: F ~> F = FunctionK.id

  implicit val FutureToIO : Future ~> IO = {
    def futureToIo[A](fa: Future[A]): IO[A] = IO.fromFuture(IO.pure(fa))

    FunctionK.lift {futureToIo}
  }
  implicit val IO_ToFuture: IO ~> Future = {
    def ioToFuture[A](ioa: IO[A]): Future[A] = ioa.unsafeToFuture()

    FunctionK.lift {ioToFuture}
  }
}
