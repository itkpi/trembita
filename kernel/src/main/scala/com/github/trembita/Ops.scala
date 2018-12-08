package com.github.trembita

import scala.language.higherKinds
import cats._
import cats.implicits._
import com.github.trembita.internal._

trait Ops[A, F[_], Ex <: Execution] extends Any {
  def self: DataPipelineT[F, A, Ex]

  def to[Ex2 <: Execution](
    implicit ex1: Ex,
    ex2: Ex2,
    F: Monad[F]
  ): DataPipelineT[F, A, Ex2] = new BridgePipelineT[F, A, Ex, Ex2](self, ex2)(ex1, F)

  /**
    * Prints each element of the pipeline
    * as a side effect
    *
    * @param toString - extract [[String]] representation of [[A]] (defaults to [[AnyRef.toString]])
    **/
  def log(
    toString: A => String = (b: A) => b.toString
  )(implicit F: Monad[F]): DataPipelineT[F, A, Ex] = self.map { a =>
    println(toString(a)); a
  }
}
