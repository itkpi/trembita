package com.github.trembita

import cats.{~>, Applicative, Eval, Functor, Id, Monad}
import scala.language.higherKinds
import cats.implicits._
import scala.annotation.implicitNotFound
import scala.collection.parallel.immutable.ParVector
import scala.reflect.ClassTag

trait ApplicativeFlatMap[F[_]] extends Serializable {
  def map[A, B: ClassTag](fa: F[A])(f: A => B): F[B]
  def mapConcat[A, B: ClassTag](fa: F[A])(f: A => Iterable[B]): F[B]
  def flatten[A: ClassTag](ffa: F[Iterable[A]]): F[A] = mapConcat(ffa)(identity)
}
trait TraverseTag[F[_], Run[_[_]]] extends Serializable {
  def traverse[G[_], A, B: ClassTag](fa: F[A])(f: A => G[B])(
      implicit G: Run[G]
  ): G[F[B]]
  def sequence[G[_]: Run, A: ClassTag](fga: F[G[A]]): G[F[A]] =
    traverse(fga)(ga => ga)

  def traverse_[G[_], A](fa: F[A])(f: A => G[Unit])(implicit G: Run[G], G0: Functor[G]): G[Unit] =
    G0.map(traverse(fa)(f))(_ => {})
}

trait Environment extends Serializable {
  type Repr[X]
  @implicitNotFound("""
    Pipeline you defined requires implicit typeclass for ${G} to be evaluated.
    Please ensure appropriate implicits are present in scope.
    For more information see Environment definition for your pipeline
    """)
  type Run[G[_]] <: Serializable
  type Result[X]
  type ResultRepr[X] = Result[Repr[X]]

  def absorbF[F[_], A](rfa: Result[F[A]])(implicit F: Monad[F], arrow: Result ~> F): F[A] =
    F.flatten(arrow(rfa))

  val FlatMapResult: Monad[Result]
  val FlatMapRepr: ApplicativeFlatMap[Repr]
  val TraverseRepr: TraverseTag[Repr, Run]

  def toVector[A](repr: Repr[A]): Result[Vector[A]]

  def foreach[A](repr: Repr[A])(f: A => Unit): Result[Unit]

  def foreachF[F[_], A](
      repr: Repr[A]
  )(f: A => F[Unit])(implicit Run: Run[F], F: Functor[F]): F[Unit] =
    TraverseRepr.traverse_[F, A](repr)(f)

  def groupBy[A, K: ClassTag](vs: Repr[A])(f: A => K): Repr[(K, Iterable[A])]

  def collect[A, B: ClassTag](repr: Repr[A])(pf: PartialFunction[A, B]): Repr[B]

  def distinctKeys[A: ClassTag, B: ClassTag](repr: Repr[(A, B)]): Repr[(A, B)]

  def concat[A](xs: Repr[A], ys: Repr[A]): Repr[A]

  def zip[A, B: ClassTag](xs: Repr[A], ys: Repr[B]): Repr[(A, B)]

  def memoize[A: ClassTag](xs: Repr[A]): Repr[A]
}

object Environment {
  type ReprAux[Repr0[_]]  = Environment { type Repr[X]   = Repr0[X] }
  type RunAux[Run0[_[_]]] = Environment { type Run[G[_]] = Run0[G] }

  sealed trait Sequential extends Environment {
    final type Repr[+X]  = Vector[X]
    final type Run[G[_]] = Applicative[G]
    final type Result[X] = X

    def toVector[A](repr: Vector[A]): Result[Vector[A]] = repr

    def collect[A, B: ClassTag](
        repr: Vector[A]
    )(pf: PartialFunction[A, B]): Vector[B] =
      repr.collect(pf)

    def fromVector[A: ClassTag](vs: Vector[A]): Vector[A] = vs

    def fromIterable[A: ClassTag](vs: Iterable[A]): Vector[A] = vs.toVector

    def fromIterator[A: ClassTag](vs: Iterator[A]): Repr[A] = vs.toVector

    def groupBy[A, K: ClassTag](
        vs: Vector[A]
    )(f: A => K): Vector[(K, Iterable[A])] =
      vs.groupBy(f).toVector

    def distinctKeys[A: ClassTag, B: ClassTag](
        repr: Repr[(A, B)]
    ): Repr[(A, B)] =
      repr.groupBy(_._1).mapValues(_.head._2).toVector

    def concat[A](xs: Vector[A], ys: Vector[A]): Vector[A] = xs ++ ys

    def zip[A, B: ClassTag](xs: Vector[A], ys: Vector[B]): Vector[(A, B)] =
      xs.zip(ys)

    def memoize[A: ClassTag](xs: Vector[A]): Vector[A] = xs

    def foreach[A](repr: Repr[A])(f: A => Unit): Result[Unit] =
      repr.foreach(f)

    val FlatMapResult: Monad[Id] = Monad[Id]

    val FlatMapRepr: ApplicativeFlatMap[Vector] =
      new ApplicativeFlatMap[Vector] {
        def pure[A: ClassTag](a: A): Vector[A]                       = Vector(a)
        def map[A, B: ClassTag](fa: Vector[A])(f: A => B): Vector[B] = fa.map(f)
        def mapConcat[A, B: ClassTag](
            fa: Vector[A]
        )(f: A => Iterable[B]): Vector[B] =
          fa.flatMap(f)
      }
    val TraverseRepr: TraverseTag[Vector, Applicative] =
      new TraverseTag[Vector, Applicative] {
        type Run[G[_]] = Applicative[G]
        def traverse[G[_], A, B: ClassTag](fa: Vector[A])(f: A => G[B])(
            implicit G: Run[G]
        ): G[Vector[B]] = cats.Traverse[Vector].traverse(fa)(f)
      }
  }

  sealed trait Parallel extends Environment {
    final type Repr[+X]  = ParVector[X]
    final type Run[G[_]] = Applicative[G]
    final type Result[X] = X

    def toVector[A](repr: ParVector[A]): Result[Vector[A]] = repr.seq

    def fromVector[A: ClassTag](vs: Vector[A]): ParVector[A] = vs.par

    def fromIterable[A: ClassTag](vs: Iterable[A]): Repr[A] = vs.to[ParVector]

    def fromIterator[A: ClassTag](vs: Iterator[A]): Repr[A] = vs.to[ParVector]

    def collect[A, B: ClassTag](repr: ParVector[A])(
        pf: PartialFunction[A, B]
    ): ParVector[B] = repr.collect(pf)

    def concat[A](xs: ParVector[A], ys: ParVector[A]): ParVector[A] = xs ++ ys

    def zip[A, B: ClassTag](xs: ParVector[A], ys: ParVector[B]): ParVector[(A, B)] =
      xs.zip(ys)

    def distinctKeys[A: ClassTag, B: ClassTag](
        repr: Repr[(A, B)]
    ): Repr[(A, B)] =
      repr.groupBy(_._1).mapValues(_.head._2).to[ParVector]

    def absorb[F[_], A](fa: Result[F[A]]): F[A] = fa

    val FlatMapResult: Monad[Id] = Monad[Id]

    val FlatMapRepr: ApplicativeFlatMap[ParVector] =
      new ApplicativeFlatMap[ParVector] {
        def pure[A: ClassTag](a: A): ParVector[A] = ParVector(a)

        def mapConcat[A, B: ClassTag](
            fa: ParVector[A]
        )(f: A => Iterable[B]): ParVector[B] =
          fa.flatMap(f)

        def map[A, B: ClassTag](fa: ParVector[A])(f: A => B): ParVector[B] =
          fa.map(f)
      }

    def groupBy[A, K: ClassTag](
        vs: ParVector[A]
    )(f: A => K): ParVector[(K, Iterable[A])] =
      vs.groupBy(f).mapValues(_.seq).toVector.par

    def memoize[A: ClassTag](xs: ParVector[A]): ParVector[A] = xs

    val TraverseRepr: TraverseTag[ParVector, Applicative] =
      new TraverseTag[ParVector, Applicative] {
        def traverse[G[_], A, B: ClassTag](
            fa: ParVector[A]
        )(f: A => G[B])(implicit G: Run[G]): G[ParVector[B]] =
          foldRight[A, G[ParVector[B]]](
            fa,
            Eval.always(G.pure(ParVector.empty))
          ) { (a, lgvb) =>
            G.map2Eval(f(a), lgvb)(_ +: _)
          }.value

        def foldRight[A, B](fa: ParVector[A], lb: Eval[B])(
            f: (A, Eval[B]) => Eval[B]
        ): Eval[B] = fa.foldRight(lb)(f)
      }

    def foreach[A](repr: Repr[A])(f: A => Unit): Result[Unit] =
      repr.foreach(f)
  }

  implicit val Parallel: Parallel     = new Parallel   {}
  implicit val Sequential: Sequential = new Sequential {}
}
