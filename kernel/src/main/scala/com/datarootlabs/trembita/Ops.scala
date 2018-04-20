package com.datarootlabs.trembita


import scala.language.higherKinds
import cats._
import cats.implicits._
import com.datarootlabs.trembita.internal._


trait Ops[A, F[_], T <: Finiteness, Ex <: Execution] extends Any {
  def self: DataPipeline[A, F, T, Ex]

  def consume(f: Either[Throwable, A] ⇒ F[Unit]): F[Unit] = self.consumeFunc[A](f)

  def to[Ex2 <: Execution](implicit ex1: Ex, ex2: Ex2, monadError: MonadError[F, Throwable]): DataPipeline[A, F, T, Ex2] = new BridgePipeline[A, F, T, Ex, Ex2](self, ex2)

  def sideEffect(f: A => Unit): DataPipeline[A, F, T, Ex] = self.map { a => f(a); a }

  def sideEffectM(f: A => F[Unit])(implicit F: Functor[F]): DataPipeline[A, F, T, Ex] = self.mapM { a => f(a).map(_ => a) }
  /**
    * Prints each element of the pipeline
    * as a side effect
    *
    * @param toString - extract [[String]] representation of [[A]] (defaults to [[AnyRef.toString]])
    **/
  def log(toString: A ⇒ String = (b: A) ⇒ b.toString): DataPipeline[A, F, T, Ex] = self.map { a ⇒ println(toString(a)); a }

  def zip[B, T2 <: Finiteness]
  (that: DataPipeline[B, F, T2, Ex])
  (implicit zp: Finiteness.Zip[T, T2],
   F: MonadError[F, Throwable],
   ex: Ex)
  : DataPipeline[(A, B), F, zp.Out, Ex] = zp(self, that)

  def ++[T2 <: Finiteness]
  (that: DataPipeline[A, F, T2, Ex])
  (implicit concat: Finiteness.Concat[T, T2],
   F: MonadError[F, Throwable],
   ex: Ex)
  : DataPipeline[A, F, concat.Out, Ex] = concat(self, that)
}
