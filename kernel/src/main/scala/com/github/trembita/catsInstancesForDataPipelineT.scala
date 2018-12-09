package com.github.trembita
import cats.{Alternative, Applicative, Eval, Id, Monad, MonadError, MonoidK, Traverse}
import cats.implicits._

import scala.language.higherKinds

trait catsInstancesForDataPipelineT {
  implicit def dataPipelineMonad[F[_], Ex <: Execution](
    implicit F: Monad[F]
  ): Monad[DataPipelineT[F, ?, Ex]] = new Monad[DataPipelineT[F, ?, Ex]] {
    def pure[A](x: A): DataPipelineT[F, A, Ex] =
      DataPipelineT.liftF[F, A, Ex](F.pure(List(x)))

    def flatMap[A, B](fa: DataPipelineT[F, A, Ex])(
      f: A => DataPipelineT[F, B, Ex]
    ): DataPipelineT[F, B, Ex] = fa.flatMap(f)

    def tailRecM[A, B](a: A)(
      f: A => DataPipelineT[F, Either[A, B], Ex]
    ): DataPipelineT[F, B, Ex] = f(a).flatMap {
      case Left(xa) => tailRecM(xa)(f)
      case Right(b) => pure(b)
    }
  }

  /**
    * Implementation of [[Monad]] for [[DataPipelineT]]
    **/
  implicit def dataPipelineTMonadError[F[_], Ex <: Execution](
    implicit F: MonadError[F, Throwable]
  ): MonadError[DataPipelineT[F, ?, Ex], Throwable] =
    new MonadError[DataPipelineT[F, ?, Ex], Throwable] {
      private val monad = dataPipelineMonad[F, Ex]
      def pure[A](x: A): DataPipelineT[F, A, Ex] =
        monad.pure(x)

      def flatMap[A, B](fa: DataPipelineT[F, A, Ex])(
        f: A => DataPipelineT[F, B, Ex]
      ): DataPipelineT[F, B, Ex] = monad.flatMap(fa)(f)

      def tailRecM[A, B](a: A)(
        f: A => DataPipelineT[F, Either[A, B], Ex]
      ): DataPipelineT[F, B, Ex] = monad.tailRecM(a)(f)

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

  implicit def dataPipelineMonoidK[F[_], Ex <: Execution](
    implicit F: Monad[F],
    Ex: Ex
  ): MonoidK[DataPipelineT[F, ?, Ex]] = new MonoidK[DataPipelineT[F, ?, Ex]] {
    def empty[A]: DataPipelineT[F, A, Ex] = DataPipelineT.empty[F, A].to[Ex]

    def combineK[A](x: DataPipelineT[F, A, Ex],
                    y: DataPipelineT[F, A, Ex]): DataPipelineT[F, A, Ex] =
      x ++ y
  }
}
