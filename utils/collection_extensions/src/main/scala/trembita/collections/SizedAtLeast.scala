package trembita.collections

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

final class SizedAtLeast[A, AtLeast <: Nat, Repr[X] <: immutable.Seq[X]](
    val unsized: Repr[A]
) {
  def :+(elem: A)(
      implicit cbf: CanBuildFrom[Repr[A], A, Repr[A]]
  ): SizedAtLeast[A, AtLeast, Repr] = {
    val builder = cbf()
    builder ++= unsized
    builder += elem
    new SizedAtLeast[A, AtLeast, Repr](builder.result())
  }
//  def +:(elem: A)(implicit cbf: CanBuildFrom[Repr[A], A, Repr[A]]): SizedAtLeast[A, AtLeast, Repr] = this.:+(elem)

  def addGrow(elem: A)(
      implicit cbf: CanBuildFrom[Repr[A], A, Repr[A]],
      sum: nat.Sum[AtLeast, _1]
  ): SizedAtLeast[A, sum.Out, Repr] = {
    val builder = cbf()
    builder ++= unsized
    builder += elem
    new SizedAtLeast[A, sum.Out, Repr](builder.result())
  }

  def ++(elems: TraversableOnce[A])(
      implicit cbf: CanBuildFrom[Repr[A], A, Repr[A]]
  ): SizedAtLeast[A, AtLeast, Repr] = {
    val builder = cbf()
    builder ++= unsized
    builder ++= elems
    new SizedAtLeast[A, AtLeast, Repr](builder.result())
  }

  def ++(elems: SizedAtLeast[A, AtLeast, Repr])(
      implicit cbf: CanBuildFrom[Repr[A], A, Repr[A]]
  ): SizedAtLeast[A, AtLeast, Repr] =
    this ++ elems.unsized

  def concatCrow[N <: Nat](elems: Sized[Repr[A], N])(
      implicit cbf: CanBuildFrom[Repr[A], A, Repr[A]],
      sum: nat.Sum[AtLeast, N]
  ): SizedAtLeast[A, sum.Out, Repr] = {
    val builder = cbf()
    builder ++= unsized
    builder ++= elems.unsized
    new SizedAtLeast[A, sum.Out, Repr](builder.result())
  }

  def apply(n: Nat)(implicit ev: nat.LT[n.N, AtLeast], toInt: nat.ToInt[n.N]): A = unsized(toInt())

  override def toString: String = s"SizedAtLeast($unsized)"
  override def equals(obj: scala.Any): Boolean = obj match {
    case y: SizedAtLeast[A, AtLeast, Repr] =>
      this.unsized.forall(a => y.unsized.contains(a))
  }
}

object SizedAtLeast {
  def apply[A, N <: Nat, Repr[X] <: immutable.Seq[X]](
      sized: Sized[Repr[A], N]
  ): SizedAtLeast[A, N, Repr] =
    new SizedAtLeast[A, N, Repr](sized.unsized)

  def apply[A, AtLeast <: Nat](n: AtLeast)(xs: A*): Wrap[A, AtLeast] =
    macro wrapImpl[A, AtLeast]

  trait Wrap[A, AtLeast <: Nat] {
    def to[Repr[X] <: immutable.Seq[X]](
        implicit cbf: CanBuildFrom[Repr[A], A, Repr[A]]
    ): SizedAtLeast[A, AtLeast, Repr] = apply[Repr]
    def apply[Repr[X] <: immutable.Seq[X]](
        implicit cbf: CanBuildFrom[Repr[A], A, Repr[A]]
    ): SizedAtLeast[A, AtLeast, Repr]
  }

  def wrapImpl[A: c.WeakTypeTag, AtLeast <: Nat: c.WeakTypeTag](
      c: blackbox.Context
  )(n: c.Expr[AtLeast])(xs: c.Expr[A]*): c.Tree = {
    import c.universe._

    val A       = weakTypeOf[A].dealias
    val AtLeast = weakTypeOf[AtLeast].dealias

    val natRegex = "shapeless.nat._([1-9][0-9]*)".r
    val succ     = typeOf[Succ[_]].typeConstructor

    def atLeast(tpe: Type, acc: Int): Int = tpe.typeConstructor match {
      case `succ` =>
        val List(p) = tpe.typeArgs
        atLeast(p, acc + 1)
      case _ =>
        tpe.toString match {
          case natRegex(int) => int.toInt + acc
          case other         => c.abort(c.enclosingPosition, s"$other is not a Nat")
        }
    }

    val minSize = atLeast(AtLeast, 0)
    var build   = q""
    for (x <- xs) build = q"$build; builder += $x"

    xs match {
      case list: List[Expr[A]] =>
        if (list.size < minSize)
          c.abort(
            c.enclosingPosition,
            s"not enough arguments,\nexpected: $minSize, actual: ${list.size}"
          )
        else {
          val expr = q"""
          new SizedAtLeast.Wrap[$A, $AtLeast] {
            def apply[Repr[X] <: scala.collection.immutable.Seq[X]]
            (implicit cbf: scala.collection.generic.CanBuildFrom[Repr[$A], $A, Repr[$A]])
            : SizedAtLeast[$A, $AtLeast, Repr] = {
              val builder = cbf()
              $build
              new SizedAtLeast[$A, $AtLeast, Repr](builder.result())
            }
          }"""
          //          println(expr)
          expr
        }
      case _ =>
        c.abort(
          c.enclosingPosition,
          s"xs must be values delimed by comma, got: $xs"
        )
    }
  }

  implicit def sizedAtLeastEq[A, AtLeast <: Nat, Repr[X] <: immutable.Seq[X]](
      implicit ev: Eq[Repr[A]]
  ): Eq[SizedAtLeast[A, AtLeast, Repr]] =
    Eq.by((_: SizedAtLeast[A, AtLeast, Repr]).unsized)

  implicit def showSizedAtLeast[A, AtLeast <: Nat, Repr[X] <: immutable.Seq[X]]: Show[
    SizedAtLeast[A, AtLeast, Repr]
  ] = macro showSizedAtLeastImpl[A, AtLeast, Repr]

  def showSizedAtLeastImpl[A: c.WeakTypeTag, AtLeast <: Nat: c.WeakTypeTag, Repr[X] <: immutable.Seq[X]](c: blackbox.Context)(
      implicit repr: c.WeakTypeTag[Repr[_]]
  ): c.Expr[Show[SizedAtLeast[A, AtLeast, Repr]]] = {
    import c.universe._

    val A       = weakTypeOf[A].dealias
    val N       = weakTypeOf[AtLeast]
    val AtLeast = N.dealias
    val Repr    = weakTypeOf[Repr[_]].dealias

    val natRegex = "shapeless.nat._([1-9][0-9]*)".r

    val n = N.toString match {
      case natRegex(num) => num
      case other         => c.abort(c.enclosingPosition, s"$other is not a Nat")
    }

    c.Expr[Show[SizedAtLeast[A, AtLeast, Repr]]](q"""
      import cats.implicits._
      new cats.Show[SizedAtLeast[$A, $AtLeast, $Repr]] {
        def show(t: SizedAtLeast[$A, $AtLeast, $Repr]): String = {
          ${Repr.toString} + "[" + ${A.toString} + "] with at least " + $n + " elements: " + t.unsized.map(_.show).mkString(", ")
        }
      }
    """)
  }
}
