package com.datarootlabs.trembita


import scala.language.higherKinds
import cats._
import cats.implicits._
import com.datarootlabs.trembita.internal._
import scala.collection.generic.CanBuildFrom


trait Ops[A, F[_], T <: Finiteness, Ex <: Execution] extends Any {
  def self: DataPipeline[A, F, T, Ex]

  def bind(f: Either[Throwable, A] ⇒ F[Unit]): F[Unit] = self.bindFunc[A](f)

  def to[Ex2 <: Execution](implicit ex1: Ex, ex2: Ex2, monadError: MonadError[F, Throwable]): DataPipeline[A, F, T, Ex2] = new BridgePipeline[A, F, T, Ex, Ex2](self, ex2)

  /**
    * Prints each element of the pipeline
    * as a side effect
    *
    * @param toString - extract [[String]] representation of [[A]] (defaults to [[AnyRef.toString]])
    **/
  def log(toString: A ⇒ String = (b: A) ⇒ b.toString): DataPipeline[A, F, T, Ex] = self.map { a ⇒ println(toString(a)); a }

  //
  //  /**
  //    * Splits a DataPipeline into N parts
  //    *
  //    * @param parts - N
  //    * @return - a pipeline of batches
  //    **/
  //  def batch(parts: Int)(implicit ev: T <:< PipelineType.Finite, F: Functor[F])
  //  : DataPipeline[Iterable[A], F, T] = new StrictSource[Iterable[A], F, T](evalFunc.map { vs ⇒
  //    ListUtils.batch(parts)(vs).toIterator
  //  })
  //
}
