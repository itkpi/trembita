package com.github.trembita.outputs.internal

import cats.{FlatMap, Monad}
import scala.annotation.implicitNotFound
import scala.language.higherKinds

@implicitNotFound("""Don't now how to keep left value for ${Out0} and ${Out1}""")
trait KeepLeft[Out0[_[_], _], Out1[_[_], _]] extends Serializable {
  def apply[F[_], A](left: Out0[F, A], right: Out1[F, A])(implicit F: Monad[F]): Out0[F, A]
}

@implicitNotFound("""Don't now how to keep right right for ${Out0} and ${Out1}""")
trait KeepRight[Out0[_[_], _], Out1[_[_], _]] extends Serializable {
  def apply[F[_], A](left: Out0[F, A], right: Out1[F, A])(implicit F: Monad[F]): Out1[F, A]
}

/*
 * ===================================================================
 *                            DSL INTERNALS
 * -------------------------------------------------------------------
 *                         ⚠️⚠️⚠️ WARNING ⚠️⚠️⚠️
 *🚨
 *                     😱😱😱   IMPLICIT HELL  😱😱😱
 *
 *                Read only home with a cup of coffee ☕
 *           Your customer needs you to feel yourself calm 😇😇😇
 *
 * ===================================================================
 * */
trait LowPriorityKeepLeft extends Serializable {
  implicit val simple: KeepLeft[λ[(G[_], a) => G[a]], λ[(G[_], a) => G[a]]] =
    new KeepLeft[λ[(G[_], a) => G[a]], λ[(G[_], a) => G[a]]] {
      def apply[F[_], A](left: F[A], right: F[A])(implicit F: Monad[F]): F[A] = F.productL(left)(right)
    }

  implicit def simpleIgnoreLeftA[U]: KeepLeft[λ[(G[_], a) => G[U]], λ[(G[_], a) => G[a]]] =
    new KeepLeft[λ[(G[_], a) => G[U]], λ[(G[_], a) => G[a]]] {
      def apply[F[_], A](left: F[U], right: F[A])(implicit F: Monad[F]): F[U] = F.productL(left)(right)
    }

  implicit def simpleIgnoreRightA[U]: KeepLeft[λ[(G[_], a) => G[a]], λ[(G[_], a) => G[U]]] =
    new KeepLeft[λ[(G[_], a) => G[a]], λ[(G[_], a) => G[U]]] {
      def apply[F[_], A](left: F[A], right: F[U])(implicit F: Monad[F]): F[A] = F.productL(left)(right)
    }

  implicit def simpleIgnoreBothA[T, U]: KeepLeft[λ[(G[_], a) => G[T]], λ[(G[_], a) => G[U]]] =
    new KeepLeft[λ[(G[_], a) => G[T]], λ[(G[_], a) => G[U]]] {
      def apply[F[_], A](left: F[T], right: F[U])(implicit F: Monad[F]): F[T] = F.productL(left)(right)
    }
}

object KeepLeft extends LowPriorityKeepLeft {

  implicit def forNestedLeft[G1[_]]: KeepLeft[λ[(G0[_], a) => G0[G1[a]]], λ[(G0[_], a) => G0[a]]] =
    new KeepLeft[λ[(G0[_], a) => G0[G1[a]]], λ[(G0[_], a) => G0[a]]] {
      def apply[F[_], A](left: F[G1[A]], right: F[A])(implicit F: Monad[F]): F[G1[A]] = F.productL(left)(right)
    }

  implicit def forNestedLeftIgnoreLeftA[G1[_], U]: KeepLeft[λ[(G0[_], a) => G0[G1[U]]], λ[(G0[_], a) => G0[a]]] =
    new KeepLeft[λ[(G0[_], a) => G0[G1[U]]], λ[(G0[_], a) => G0[a]]] {
      def apply[F[_], A](left: F[G1[U]], right: F[A])(implicit F: Monad[F]): F[G1[U]] = F.productL(left)(right)
    }

  implicit def forNestedLeftIgnoreRightA[G1[_], U]: KeepLeft[λ[(G0[_], a) => G0[G1[a]]], λ[(G0[_], a) => G0[U]]] =
    new KeepLeft[λ[(G0[_], a) => G0[G1[a]]], λ[(G0[_], a) => G0[U]]] {
      def apply[F[_], A](left: F[G1[A]], right: F[U])(implicit F: Monad[F]): F[G1[A]] = F.productL(left)(right)
    }

  implicit def forNestedRight[G1[_]]: KeepLeft[λ[(G0[_], a) => G0[a]], λ[(G0[_], a) => G0[G1[a]]]] =
    new KeepLeft[λ[(G0[_], a) => G0[a]], λ[(G0[_], a) => G0[G1[a]]]] {
      def apply[F[_], A](left: F[A], right: F[G1[A]])(implicit F: Monad[F]): F[A] = F.productL(left)(right)
    }

  implicit def forNestedRightIgnoreLeftA[G1[_], U]: KeepLeft[λ[(G0[_], a) => G0[U]], λ[(G0[_], a) => G0[G1[a]]]] =
    new KeepLeft[λ[(G0[_], a) => G0[U]], λ[(G0[_], a) => G0[G1[a]]]] {
      def apply[F[_], A](left: F[U], right: F[G1[A]])(implicit F: Monad[F]): F[U] = F.productL(left)(right)
    }

  implicit def forNestedRightIgnoreRightA[G1[_], U]: KeepLeft[λ[(G0[_], a) => G0[a]], λ[(G0[_], a) => G0[G1[U]]]] =
    new KeepLeft[λ[(G0[_], a) => G0[a]], λ[(G0[_], a) => G0[G1[U]]]] {
      def apply[F[_], A](left: F[A], right: F[G1[U]])(implicit F: Monad[F]): F[A] = F.productL(left)(right)
    }

  implicit def forNestedBoth[G1[_], G2[_]]: KeepLeft[λ[(G0[_], a) => G0[G1[a]]], λ[(G0[_], a) => G0[G2[a]]]] =
    new KeepLeft[λ[(G0[_], a) => G0[G1[a]]], λ[(G0[_], a) => G0[G2[a]]]] {
      def apply[F[_], A](left: F[G1[A]], right: F[G2[A]])(implicit F: Monad[F]): F[G1[A]] = F.productL(left)(right)
    }

  implicit def forNestedBothIgnoreLeftA[G1[_], G2[_], U]: KeepLeft[λ[(G0[_], a) => G0[G1[U]]], λ[(G0[_], a) => G0[G2[a]]]] =
    new KeepLeft[λ[(G0[_], a) => G0[G1[U]]], λ[(G0[_], a) => G0[G2[a]]]] {
      def apply[F[_], A](left: F[G1[U]], right: F[G2[A]])(implicit F: Monad[F]): F[G1[U]] = F.productL(left)(right)
    }

  implicit def forNestedBothIgnoreRightA[G1[_], G2[_], U]: KeepLeft[λ[(G0[_], a) => G0[G1[a]]], λ[(G0[_], a) => G0[G2[U]]]] =
    new KeepLeft[λ[(G0[_], a) => G0[G1[a]]], λ[(G0[_], a) => G0[G2[U]]]] {
      def apply[F[_], A](left: F[G1[A]], right: F[G2[U]])(implicit F: Monad[F]): F[G1[A]] = F.productL(left)(right)
    }
}

trait LowPriorityKeepRight extends Serializable {
  implicit val forFlatMap: KeepRight[λ[(G[_], a) => G[a]], λ[(G[_], a) => G[a]]] =
    new KeepRight[λ[(G[_], a) => G[a]], λ[(G[_], a) => G[a]]] {
      def apply[F[_], A](left: F[A], right: F[A])(implicit F: Monad[F]): F[A] = F.productR(left)(right)
    }

  implicit def simpleIgnoreLeftA[U]: KeepRight[λ[(G[_], a) => G[U]], λ[(G[_], a) => G[a]]] =
    new KeepRight[λ[(G[_], a) => G[U]], λ[(G[_], a) => G[a]]] {
      def apply[F[_], A](left: F[U], right: F[A])(implicit F: Monad[F]): F[A] = F.productR(left)(right)
    }

  implicit def simpleIgnoreRightA[U]: KeepRight[λ[(G[_], a) => G[a]], λ[(G[_], a) => G[U]]] =
    new KeepRight[λ[(G[_], a) => G[a]], λ[(G[_], a) => G[U]]] {
      def apply[F[_], A](left: F[A], right: F[U])(implicit F: Monad[F]): F[U] = F.productR(left)(right)
    }

  implicit def simpleIgnoreBothA[T, U]: KeepRight[λ[(G[_], a) => G[T]], λ[(G[_], a) => G[U]]] =
    new KeepRight[λ[(G[_], a) => G[T]], λ[(G[_], a) => G[U]]] {
      def apply[F[_], A](left: F[T], right: F[U])(implicit F: Monad[F]): F[U] = F.productR(left)(right)
    }
}

object KeepRight extends LowPriorityKeepRight {
  implicit def forNestedLeft[G1[_]]: KeepRight[λ[(G0[_], a) => G0[G1[a]]], λ[(G0[_], a) => G0[a]]] =
    new KeepRight[λ[(G0[_], a) => G0[G1[a]]], λ[(G0[_], a) => G0[a]]] {
      def apply[F[_], A](left: F[G1[A]], right: F[A])(implicit F: Monad[F]): F[A] = F.productR(left)(right)
    }

  implicit def forNestedLeftIgnoreLeftA[G1[_], U]: KeepRight[λ[(G0[_], a) => G0[G1[U]]], λ[(G0[_], a) => G0[a]]] =
    new KeepRight[λ[(G0[_], a) => G0[G1[U]]], λ[(G0[_], a) => G0[a]]] {
      def apply[F[_], A](left: F[G1[U]], right: F[A])(implicit F: Monad[F]): F[A] = F.productR(left)(right)
    }

  implicit def forNestedLeftIgnoreRightA[G1[_], U]: KeepRight[λ[(G0[_], a) => G0[G1[a]]], λ[(G0[_], a) => G0[U]]] =
    new KeepRight[λ[(G0[_], a) => G0[G1[a]]], λ[(G0[_], a) => G0[U]]] {
      def apply[F[_], A](left: F[G1[A]], right: F[U])(implicit F: Monad[F]): F[U] = F.productR(left)(right)
    }

  implicit def forNestedRight[G1[_]]: KeepRight[λ[(G0[_], a) => G0[a]], λ[(G0[_], a) => G0[G1[a]]]] =
    new KeepRight[λ[(G0[_], a) => G0[a]], λ[(G0[_], a) => G0[G1[a]]]] {
      def apply[F[_], A](left: F[A], right: F[G1[A]])(implicit F: Monad[F]): F[G1[A]] = F.productR(left)(right)
    }

  implicit def forNestedRightIgnoreLeftA[G1[_], U]: KeepRight[λ[(G0[_], a) => G0[U]], λ[(G0[_], a) => G0[G1[a]]]] =
    new KeepRight[λ[(G0[_], a) => G0[U]], λ[(G0[_], a) => G0[G1[a]]]] {
      def apply[F[_], A](left: F[U], right: F[G1[A]])(implicit F: Monad[F]): F[G1[A]] = F.productR(left)(right)
    }

  implicit def forNestedRightIgnoreRightA[G1[_], U]: KeepRight[λ[(G0[_], a) => G0[a]], λ[(G0[_], a) => G0[G1[U]]]] =
    new KeepRight[λ[(G0[_], a) => G0[a]], λ[(G0[_], a) => G0[G1[U]]]] {
      def apply[F[_], A](left: F[A], right: F[G1[U]])(implicit F: Monad[F]): F[G1[U]] = F.productR(left)(right)
    }

  implicit def forNestedBoth[G1[_], G2[_]]: KeepRight[λ[(G0[_], a) => G0[G1[a]]], λ[(G0[_], a) => G0[G2[a]]]] =
    new KeepRight[λ[(G0[_], a) => G0[G1[a]]], λ[(G0[_], a) => G0[G2[a]]]] {
      def apply[F[_], A](left: F[G1[A]], right: F[G2[A]])(implicit F: Monad[F]): F[G2[A]] = F.productR(left)(right)
    }

  implicit def forNestedBothIgnoreLeftA[G1[_], G2[_], U]: KeepRight[λ[(G0[_], a) => G0[G1[U]]], λ[(G0[_], a) => G0[G2[a]]]] =
    new KeepRight[λ[(G0[_], a) => G0[G1[U]]], λ[(G0[_], a) => G0[G2[a]]]] {
      def apply[F[_], A](left: F[G1[U]], right: F[G2[A]])(implicit F: Monad[F]): F[G2[A]] = F.productR(left)(right)
    }

  implicit def forNestedBothIgnoreRightA[G1[_], G2[_], U]: KeepRight[λ[(G0[_], a) => G0[G1[a]]], λ[(G0[_], a) => G0[G2[U]]]] =
    new KeepRight[λ[(G0[_], a) => G0[G1[a]]], λ[(G0[_], a) => G0[G2[U]]]] {
      def apply[F[_], A](left: F[G1[A]], right: F[G2[U]])(implicit F: Monad[F]): F[G2[U]] = F.productR(left)(right)

    }
}
