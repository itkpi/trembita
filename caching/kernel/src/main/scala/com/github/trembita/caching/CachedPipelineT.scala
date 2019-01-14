package com.github.trembita.caching

import cats.Monad
import com.github.trembita.internal.SeqSource
import com.github.trembita.{DataPipelineT, Environment}
import scala.language.higherKinds
import scala.reflect.ClassTag

object CachedPipelineT {
  def make[F[_], A: ClassTag, E <: Environment](
      source: DataPipelineT[F, A, E],
      cacheName: String
  )(implicit F: Monad[F], caching: Caching[F, E, A]): DataPipelineT[F, A, E] =
    new SeqSource[F, A, E](F) {
      protected[trembita] def evalFunc[B >: A](Ex: E)(implicit run: Ex.Run[F]): F[Ex.Repr[B]] =
        caching.cached(cacheName, source.evalFunc[A](Ex).asInstanceOf[F[E#Repr[A]]]).asInstanceOf[F[Ex.Repr[B]]]
    }
}
