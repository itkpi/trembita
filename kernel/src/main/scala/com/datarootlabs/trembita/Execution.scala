package com.datarootlabs.trembita


import scala.language.higherKinds
import cats._
import cats.implicits._
import scala.collection.parallel.immutable.ParVector

trait Execution {
  type Repr[X]
  val Monad   : Monad[Repr]
  val Traverse: Traverse[Repr]

  def toVector[A](repr: Repr[A]): Vector[A]

  def fromVector[A](vs: Vector[A]): Repr[A]

  def fromIterable[A](vs: Iterable[A]): Repr[A]

  def groupBy[A, K](vs: Repr[A])(f: A => K): Map[K, Repr[A]]

  def collect[A, B](repr: Repr[A])(pf: PartialFunction[A, B]): Repr[B]

  def sorted[A: Ordering](repr: Repr[A]): Repr[A]

  def concat[A](xs: Repr[A], ys: Repr[A]): Repr[A]

  def zip[A, B](xs: Repr[A], ys: Repr[B]): Repr[(A, B)]
}

object Execution {

  sealed trait Sequential extends Execution {
    type Repr[X] = Vector[X]

    def toVector[A](repr: Vector[A]): Vector[A] = repr

    def collect[A, B](repr: Vector[A])(pf: PartialFunction[A, B]): Vector[B] = repr.collect(pf)

    def fromVector[A](vs: Vector[A]): Vector[A] = vs

    def fromIterable[A](vs: Iterable[A]): Vector[A] = vs.toVector

    def groupBy[A, K](vs: Vector[A])(f: A => K): Map[K, Vector[A]] = vs.groupBy(f)

    def sorted[A: Ordering](vs: Vector[A]): Vector[A] = vs.sorted

    def concat[A](xs: Vector[A], ys: Vector[A]): Vector[A] = xs ++ ys

    def zip[A, B](xs: Vector[A], ys: Vector[B]): Vector[(A, B)] = xs.zip(ys)

    val Monad   : Monad[Vector]    = cats.Monad[Vector]
    val Traverse: Traverse[Vector] = cats.Traverse[Vector]
  }

  sealed trait Parallel extends Execution {
    type Repr[X] = ParVector[X]

    def toVector[A](repr: ParVector[A]): Vector[A] = repr.seq

    def fromVector[A](vs: Vector[A]): ParVector[A] = vs.par

    def fromIterable[A](vs: Iterable[A]): Repr[A] = vs.toVector.par

    def collect[A, B](repr: ParVector[A])(pf: PartialFunction[A, B]): ParVector[B] = repr.collect(pf)

    def sorted[A: Ordering](vs: ParVector[A]): ParVector[A] = vs.seq.sorted.par

    def concat[A](xs: ParVector[A], ys: ParVector[A]): ParVector[A] = xs ++ ys

    def zip[A, B](xs: ParVector[A], ys: ParVector[B]): ParVector[(A, B)] = xs.zip(ys)

    val Monad: Monad[ParVector] = new Monad[ParVector] {
      def pure[A](a: A): ParVector[A] = ParVector(a)

      def flatMap[A, B](fa: ParVector[A])(f: A => ParVector[B]): ParVector[B] = fa.flatMap(f)

      def tailRecM[A, B](a: A)(f: A => ParVector[Either[A, B]]): ParVector[B] = f(a).flatMap {
        case Left(ax) => tailRecM(ax)(f)
        case Right(b) => ParVector(b)
      }

      override def map[A, B](fa: ParVector[A])(f: A => B): ParVector[B] = fa.map(f)
    }

    def groupBy[A, K](vs: ParVector[A])(f: A => K): Map[K, ParVector[A]] = vs.groupBy(f).seq

    val Traverse: Traverse[ParVector] = new Traverse[ParVector] {
      def traverse[G[_], A, B](fa: ParVector[A])(f: A => G[B])(implicit G: Applicative[G]): G[ParVector[B]] = {
        foldRight[A, G[ParVector[B]]](fa, Always(G.pure(ParVector.empty))) { (a, lgvb) =>
          G.map2Eval(f(a), lgvb)(_ +: _)
        }.value
      }

      def foldLeft[A, B](fa: ParVector[A], b: B)(f: (B, A) => B): B = fa.foldLeft(b)(f)

      def foldRight[A, B](fa: ParVector[A], lb: Eval[B])(f: (A, Eval[B]) => Eval[B]): Eval[B] = fa.foldRight(lb)(f)
    }
  }

  implicit val Parallel  : Parallel   = new Parallel {}
  implicit val Sequential: Sequential = new Sequential {}
}
