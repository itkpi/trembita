package com.github

import scala.language.{higherKinds, implicitConversions}
import cats._
import cats.implicits._
import cats.arrow._
import cats.data.Kleisli
import cats.effect.{Effect, IO, Sync}
import com.github.trembita.internal._
import scala.collection.generic.CanBuildFrom
import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

package object trembita {

  type DataPipeline[A, Ex <: Execution] = DataPipelineT[Id, A, Ex]
  object DataPipeline {

    /**
      * Wraps given elements into a [[DataPipelineT]]
      *
      * @param xs - elements to wrap
      * @return - a [[StrictSource]]
      **/
    def apply[A](xs: A*): DataPipeline[A, Execution.Sequential] =
      new StrictSource[Id, A, Execution.Sequential](xs.toIterator, Monad[Id])

    /**
      * Wraps an [[Iterable]] passed by-name
      *
      * @param it - an iterable haven't been evaluated yet
      * @return - a [[StrictSource]]
      **/
    def from[A](it: => Iterable[A]): DataPipeline[A, Execution.Sequential] =
      new StrictSource[Id, A, Execution.Sequential](it.toIterator, Monad[Id])
  }
  type PairPipelineT[F[_], K, V, Ex <: Execution] = DataPipelineT[F, (K, V), Ex]

  /**
    * Operations for [[DataPipelineT]] of tuples
    * (NOT [[MapPipelineT]])
    **/
  implicit class PairPipelineOps[F[_], K, V, Ex <: Execution](
    val self: PairPipelineT[F, K, V, Ex]
  ) extends AnyVal {
    def mapValues[W](
      f: V => W
    )(implicit F: Monad[F]): PairPipelineT[F, K, W, Ex] = self.map {
      case (k, v) => (k, f(v))
    }

    def keys(implicit F: Monad[F]): DataPipelineT[F, K, Ex] = self.map(_._1)

    def values(implicit F: Monad[F]): DataPipelineT[F, V, Ex] = self.map(_._2)

    /**
      * Merges all values [[V]]
      * with the same key [[K]]
      *
      * @param f - reducing function
      * @return - reduced pipeline
      **/
    def reduceByKey(f: (V, V) => V)(implicit ex: Ex,
                                    F: Monad[F]): PairPipelineT[F, K, V, Ex] =
      self.groupBy(_._1).mapValues { vs =>
        vs.foldLeft(Option.empty[V]) {
            case (None, (_, v)) => Some(v)
            case (acc, (_, v))  => acc.map(f(_, v))
          }
          .get
      }

    /**
      * Same to [[reduceByKey]] above
      * Merges all values [[V]]
      * having a [[Monoid]] defined
      *
      * @param vMonoid - Monoid for [[V]]
      * @return - reduced pipeline
      **/
    def reduceByKey(implicit vMonoid: Monoid[V],
                    ex: Ex,
                    F: Monad[F]): PairPipelineT[F, K, V, Ex] =
      reduceByKey((v1, v2) => vMonoid.combine(v1, v2))

    /** @return - [[MapPipelineT]] */
    def toMap(implicit ex: Ex, F: Monad[F]): MapPipelineT[F, K, V, Ex] =
      new BaseMapPipelineT[F, K, V, Ex](
        self.asInstanceOf[DataPipelineT[F, (K, V), Ex]],
        ex,
        F
      )
  }

  /**
    * Implementation of [[Monad]] for [[DataPipelineT]]
    **/
  implicit def dataPipelineTMonadError[F[_], Ex <: Execution](
    implicit F: MonadError[F, Throwable]
  ): MonadError[DataPipelineT[F, ?, Ex], Throwable] =
    new MonadError[DataPipelineT[F, ?, Ex], Throwable] {
      def pure[A](x: A): DataPipelineT[F, A, Ex] =
        DataPipelineT.liftF[F, A, Ex]((List(x): Iterable[A]).pure[F])

      def flatMap[A, B](fa: DataPipelineT[F, A, Ex])(
        f: A => DataPipelineT[F, B, Ex]
      ): DataPipelineT[F, B, Ex] = fa.flatMap(f)

      def tailRecM[A, B](a: A)(
        f: A => DataPipelineT[F, Either[A, B], Ex]
      ): DataPipelineT[F, B, Ex] = f(a).flatMap {
        case Left(xa) => tailRecM(xa)(f)
        case Right(b) => pure(b)
      }

      def raiseError[A](e: Throwable): DataPipelineT[F, A, Ex] =
        DataPipelineT.liftF(F.raiseError[Iterable[A]](e))

      override def handleError[A](fa: DataPipelineT[F, A, Ex])(
        f: Throwable => A
      ): DataPipelineT[F, A, Ex] = fa.handleError(f)

      def handleErrorWith[A](
        fa: DataPipelineT[F, A, Ex]
      )(f: Throwable => DataPipelineT[F, A, Ex]): DataPipelineT[F, A, Ex] =
        fa.handleErrorWith(f)
    }

  implicit class PipelineOps[A, F[_], Ex <: Execution](
    val self: DataPipelineT[F, A, Ex]
  ) extends AnyVal
      with Ops[A, F, Ex]
      with Ops2[A, F, Ex]

  implicit def iterable2DataPipeline[A, F[_], Ex <: Execution](
    iterable: Iterable[A]
  )(implicit F: Monad[F]): DataPipelineT[F, A, Ex] =
    DataPipelineT.liftF[F, A, Ex](iterable.pure[F])

  implicit def array2DataPipeline[A, F[_], Ex <: Execution](
    array: Array[A]
  )(implicit F: Monad[F]): DataPipelineT[F, A, Ex] =
    DataPipelineT.liftF[F, A, Ex]((array: Iterable[A]).pure[F])

  implicit def TryT[G[_]](implicit G: MonadError[G, Throwable]): Try ~> G = {
    def tryToG[A](tryA: Try[A]): G[A] = tryA match {
      case Success(v) => G.pure(v)
      case Failure(e) => G.raiseError(e)
    }

    FunctionK.lift { tryToG }
  }

  implicit def IdT[F[_]]: F ~> F = FunctionK.id

  implicit val FutureToIO: Future ~> IO = {
    def futureToIo[A](fa: Future[A]): IO[A] = IO.fromFuture(IO.pure(fa))

    FunctionK.lift { futureToIo }
  }
  implicit val IO_ToFuture: IO ~> Future = {
    def ioToFuture[A](ioa: IO[A]): Future[A] = ioa.unsafeToFuture()

    FunctionK.lift { ioToFuture }
  }
}
