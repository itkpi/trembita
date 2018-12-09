package com.github.trembita

import scala.language.higherKinds
import cats._
import cats.implicits._
import com.github.trembita.internal._

trait Ops1[A, F[_], Ex <: Execution] extends Any {
  def self: DataPipelineT[F, A, Ex]

  def to[Ex2 <: Execution](implicit ex1: Ex,
                           ex2: Ex2,
                           F: Monad[F]): DataPipelineT[F, A, Ex2] =
    new BridgePipelineT[F, A, Ex, Ex2](self, ex2)(ex1, F)

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

  def join[B](that: DataPipelineT[F, B, Ex])(
    on: (A, B) => Boolean
  )(implicit F: Monad[F], Ex: Ex): DataPipelineT[F, (A, B), Ex] =
    new StrictSource[F, (A, B), Ex]({
      for {
        self <- self.eval
        that <- that.eval
      } yield {
        self.iterator.flatMap { a =>
          that.collectFirst {
            case b if on(a, b) => a -> b
          }
        }
      }
    }, F)

  def joinLeft[B](that: DataPipelineT[F, B, Ex])(
    on: (A, B) => Boolean
  )(implicit F: Monad[F], Ex: Ex): DataPipelineT[F, (A, Option[B]), Ex] =
    new StrictSource[F, (A, Option[B]), Ex]({
      for {
        self <- self.eval
        that <- that.eval
      } yield {
        self.iterator.map { a =>
          a -> that.collectFirst {
            case b if on(a, b) => b
          }
        }
      }
    }, F)

  def joinRight[B](that: DataPipelineT[F, B, Ex])(
    on: (A, B) => Boolean
  )(implicit F: Monad[F], Ex: Ex): DataPipelineT[F, (Option[A], B), Ex] =
    new StrictSource[F, (Option[A], B), Ex]({
      for {
        self <- self.eval
        that <- that.eval
      } yield {
        that.iterator.map { b =>
          self.collectFirst {
            case a if on(a, b) => a
          } -> b
        }
      }
    }, F)

  def joinFull[B](that: DataPipelineT[F, B, Ex])(on: (A, B) => Boolean)(
    implicit F: Monad[F],
    Ex: Ex
  ): DataPipelineT[F, (Option[A], Option[B]), Ex] =
    new StrictSource[F, (Option[A], Option[B]), Ex](
      {
        for {
          self <- self.eval
          that <- that.eval
        } yield {
          self.iterator.map { a =>
            Some(a) -> that.collectFirst {
              case b if on(a, b) => b
            }
          } ++ that.iterator.map { b =>
            self.collectFirst {
              case a if on(a, b) => a
            } -> Some(b)
          }
        }
      },
      F
    )

  def ++(that: DataPipelineT[F, A, Ex])(implicit F: Monad[F],
                                        Ex: Ex): DataPipelineT[F, A, Ex] =
    new StrictSource[F, A, Ex](for {
      self <- self.eval
      that <- that.eval
    } yield self.iterator ++ that.iterator, F)

  def zip[B](
    that: DataPipelineT[F, B, Ex]
  )(implicit F: Monad[F], Ex: Ex): DataPipelineT[F, (A, B), Ex] =
    new StrictSource[F, (A, B), Ex](for {
      self <- self.eval
      that <- that.eval
    } yield self.iterator zip that.iterator, F)

  def zipWithIndex(implicit F: Monad[F],
                   Ex: Ex): DataPipelineT[F, (A, Int), Ex] =
    new StrictSource[F, (A, Int), Ex](
      self.eval.map(_.toIterator.zipWithIndex),
      F
    )

  def :++(fa: F[A])(implicit F: Monad[F], Ex: Ex): DataPipelineT[F, A, Ex] =
    self ++ DataPipelineT.liftF[F, A, Ex](fa.map(List(_)))

  def :+(a: A)(implicit F: Monad[F], Ex: Ex): DataPipelineT[F, A, Ex] =
    self :++ F.pure(a)

  def ++:(fa: F[A])(implicit F: Monad[F], Ex: Ex): DataPipelineT[F, A, Ex] =
    DataPipelineT.liftF[F, A, Ex](fa.map(List(_))) ++ self

  def +:(a: A)(implicit F: Monad[F], Ex: Ex): DataPipelineT[F, A, Ex] =
    F.pure(a) ++: self

  def mapK[G[_]](
    funcK: F ~> G
  )(implicit F: Functor[F], G: Monad[G], Ex: Ex): DataPipelineT[G, A, Ex] =
    new StrictSource[G, A, Ex](funcK(self.eval).map(_.iterator), G)
}
