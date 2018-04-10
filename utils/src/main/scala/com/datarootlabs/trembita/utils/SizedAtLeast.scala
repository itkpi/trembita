package com.datarootlabs.trembita.utils

import cats._
import cats.implicits._
import shapeless._
import shapeless.nat._
import shapeless.ops.nat

import scala.collection.generic.CanBuildFrom
import scala.collection.immutable
import scala.language.experimental.macros
import scala.language.{higherKinds, implicitConversions}
import scala.reflect.macros.blackbox


final class SizedAtLeast[A, AtLeast <: Nat, Repr[X] <: immutable.Seq[X]] protected[trembita](val unsized: Repr[A]) {
  def :+(elem: A)(implicit cbf: CanBuildFrom[Repr[A], A, Repr[A]]): SizedAtLeast[A, AtLeast, Repr] = {
    val builder = cbf()
    builder ++= unsized
    builder += elem
    new SizedAtLeast[A, AtLeast, Repr](builder.result())
  }
  def +:(elem: A)(implicit cbf: CanBuildFrom[Repr[A], A, Repr[A]]): SizedAtLeast[A, AtLeast, Repr] = this.:+(elem)

  def addGrow(elem: A)(implicit cbf: CanBuildFrom[Repr[A], A, Repr[A]], sum: nat.Sum[AtLeast, _1])
  : SizedAtLeast[A, sum.Out, Repr] = {
    val builder = cbf()
    builder ++= unsized
    builder += elem
    new SizedAtLeast[A, sum.Out, Repr](builder.result())
  }

  def ++(elems: TraversableOnce[A])(implicit cbf: CanBuildFrom[Repr[A], A, Repr[A]]): SizedAtLeast[A, AtLeast, Repr] = {
    val builder = cbf()
    builder ++= unsized
    builder ++= elems
    new SizedAtLeast[A, AtLeast, Repr](builder.result())
  }

  def concatCrow[N <: Nat](elems: Sized[Repr[A], N])(implicit cbf: CanBuildFrom[Repr[A], A, Repr[A]], sum: nat.Sum[AtLeast, N])
  : SizedAtLeast[A, sum.Out, Repr] = {
    val builder = cbf()
    builder ++= unsized
    builder ++= elems.unsized
    new SizedAtLeast[A, sum.Out, Repr](builder.result())
  }

  def apply(n: Nat)(implicit ev: nat.LT[n.N, AtLeast], toInt: nat.ToInt[n.N]): A = unsized(toInt())
}

object SizedAtLeast {
  def apply[A, N <: Nat, Repr[X] <: immutable.Seq[X]](sized: Sized[Repr[A], N]): SizedAtLeast[A, N, Repr] =
    new SizedAtLeast[A, N, Repr](sized.unsized)

  def apply[A, N <: Nat, Repr[X] <: immutable.Seq[X]]
  (n: N)(xs: A*)
  (implicit toInt: nat.ToInt[N], cbf: CanBuildFrom[Repr[A], A, Repr[A]])
  : SizedAtLeast[A, N, Repr] = macro applyImpl[A, N, Repr]

  def applyImpl[A: c.WeakTypeTag, N <: Nat : c.WeakTypeTag, Repr[X] <: immutable.Seq[X]]
  (c: blackbox.Context)(n: c.Expr[N])(xs: c.Expr[A]*)
  (toInt: c.Expr[nat.ToInt[N]],
   cbf: c.Expr[CanBuildFrom[Repr[A], A, Repr[A]]]): c.Expr[SizedAtLeast[A, N, Repr]] = {
    import c.universe._

    val A = weakTypeOf[A].dealias
    val N = weakTypeOf[N].dealias

    (xs, n, toInt) match {
      case (Constant(Literal(list: immutable.Seq[A])), Constant(Literal(nx: N)), Constant(Literal(toIntx: nat.ToInt[N]))) ⇒
        if (list.lengthCompare(toIntx()) >= 0) reify {
          new SizedAtLeast[A, N, Repr]((cbf.value() ++= list).result())
        } else throw new IllegalArgumentException(s"SizedAtLeast.apply($N)(...) takes at least $N values (${list.size} given)")
      case _                                                                     ⇒
        throw new IllegalArgumentException(s"SizedAtLeast.apply takes only literals")
    }
  }

  //  implicit def sizedAtLeastToRepr[A, AtLeast <: Nat, Repr[_] <: immutable.Seq[_]]
  //  (sizedAtLeast: SizedAtLeast[A, AtLeast, Repr])
  //  : Repr[A] = sizedAtLeast.unsized

  implicit def sizedAtLeastEq[A: Eq, AtLeast <: Nat, Repr[X] <: immutable.Seq[X]]: Eq[SizedAtLeast[A, AtLeast, Repr]] =
    new Eq[SizedAtLeast[A, AtLeast, Repr]] {
      def eqv(x: SizedAtLeast[A, AtLeast, Repr], y: SizedAtLeast[A, AtLeast, Repr]): Boolean = {
        x.unsized.forall(a ⇒ y.unsized.exists(_ === a))
      }
    }
}