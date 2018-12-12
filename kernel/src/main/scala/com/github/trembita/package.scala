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

package object trembita extends AllSyntax {

  type DataPipeline[A, Ex <: Execution] = DataPipelineT[Id, A, Ex]
  object DataPipeline {

    /**
      * Wraps given elements into a [[DataPipelineT]]
      *
      * @param xs - elements to wrap
      * @return - a [[StrictSource]]
      **/
    def apply[A: ClassTag](xs: A*): DataPipeline[A, Execution.Sequential] =
      new StrictSource[Id, A, Execution.Sequential](xs.toIterator, Monad[Id])

    /**
      * Wraps an [[Iterable]] passed by-name
      *
      * @param it - an iterable haven't been evaluated yet
      * @return - a [[StrictSource]]
      **/
    def from[A: ClassTag](
      it: => Iterable[A]
    ): DataPipeline[A, Execution.Sequential] =
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

    def keys(implicit F: Monad[F], K: ClassTag[K]): DataPipelineT[F, K, Ex] =
      self.map(_._1)

    def values(implicit F: Monad[F], V: ClassTag[V]): DataPipelineT[F, V, Ex] =
      self.map(_._2)

    /**
      * Merges all values [[V]]
      * with the same key [[K]]
      *
      * @param f - reducing function
      * @return - reduced pipeline
      **/
    def reduceByKey(f: (V, V) => V)(implicit ex: Ex,
                                    K: ClassTag[K],
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
                    K: ClassTag[K],
                    ex: Ex,
                    F: Monad[F]): PairPipelineT[F, K, V, Ex] =
      reduceByKey((v1, v2) => vMonoid.combine(v1, v2))

    /** @return - [[MapPipelineT]] */
    def toMap(implicit K: ClassTag[K],
              V: ClassTag[V],
              F: Monad[F]): MapPipelineT[F, K, V, Ex] =
      new BaseMapPipelineT[F, K, V, Ex](
        self.asInstanceOf[DataPipelineT[F, (K, V), Ex]],
        F
      )
  }

  implicit def iterable2DataPipeline[A: ClassTag, F[_], Ex <: Execution](
    iterable: Iterable[A]
  )(implicit F: Monad[F]): DataPipelineT[F, A, Ex] =
    DataPipelineT.liftF[F, A, Ex](iterable.pure[F])

  implicit def array2DataPipeline[A: ClassTag, F[_], Ex <: Execution](
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
  implicit val IO2Try: IO ~> Try = {
    def ioToTry[A](ioa: IO[A]): Try[A] = Try { ioa.unsafeRunSync() }

    FunctionK.lift { ioToTry }
  }

  type Sequential = Execution.Sequential
  type Parallel = Execution.Parallel
}
