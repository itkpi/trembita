package com.github.trembita.outputs.internal

import cats.Monad
import scala.language.higherKinds

trait lowPriorityTricks {
  implicit def chainTuples[A, B](implicit p0: A, p1: B): (A, B)                    = (p0, p1)
//  implicit def tupleMonad[F[_]](implicit F: Monad[F]): Monad[λ[a => (F[a], F[a])]] = ???
}
