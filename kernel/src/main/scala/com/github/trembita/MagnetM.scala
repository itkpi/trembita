package com.github.trembita

import scala.language.higherKinds
import scala.language.implicitConversions

trait MagnetM[F[_], A, B, Ex <: Execution] {
  def prepared: A => F[B]
}

trait standardMagnets {
  implicit def prepareSequential[F[_], A, B](f: A => F[B]): MagnetM[F, A, B, Sequential] = new MagnetM[F, A, B, Sequential] {
    def prepared: A => F[B] = f
  }

  implicit def prepareParallel[F[_], A, B](f: A => F[B]): MagnetM[F, A, B, Parallel] = new MagnetM[F, A, B, Parallel] {
    def prepared: A => F[B] = f
  }
}
