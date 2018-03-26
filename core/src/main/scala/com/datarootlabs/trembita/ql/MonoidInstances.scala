package com.datarootlabs.trembita.ql


import cats.Monoid
import shapeless._
import scala.language.experimental.macros
import scala.language.higherKinds
import scala.reflect.macros.blackbox
import ArbitraryGroupResult._
import GroupingCriteria._
import Aggregation._
import com.datarootlabs.trembita.utils._
import shapeless._
import cats.implicits._


trait MonoidInstances {
  implicit def taggedMonoid[A, T](implicit aMonoid: Monoid[A]): Monoid[A ## T] = new Monoid[A ## T] {
    override def empty: A ## T = new ##[A, T](aMonoid.empty)
    override def combine(x: A ## T, y: A ## T): A ## T = new ##[A, T](aMonoid.combine(x.value, y.value))
  }
  def hListMonoidImpl[K <: HList : c.WeakTypeTag](c: blackbox.Context): c.Expr[Monoid[K]] = {
    import c.universe._

    val hnil = typeOf[HNil].dealias
    val K = weakTypeOf[K].dealias

    val expr = K match {
      case `hnil` ⇒ q"HNilMonoid"
      case _      ⇒ K.typeArgs match {
        case List(head, tail) ⇒ q"hlistMonoid[$head, $tail]"
        case other            ⇒ throw new IllegalArgumentException(s"Unexpected $K in HList Monoid macro")
      }
    }
    c.Expr[Monoid[K]](expr)
  }

  object HNilMonoid extends Monoid[HNil] {
    override def empty: HNil = HNil
    override def combine(h1: HNil, h2: HNil): HNil = HNil
  }

  def hlistMonoid[H: Monoid, T <: HList : Monoid]: Monoid[H :: T] =
    new Monoid[H :: T] {
      private val headMonoid = Monoid[H]
      private val tailMonoid = Monoid[T]

      override def empty: H :: T = headMonoid.empty :: tailMonoid.empty
      override def combine(hlist1: H :: T, hlist2: H :: T): H :: T =
        headMonoid.combine(hlist1.head, hlist2.head) :: tailMonoid.combine(hlist1.tail, hlist2.tail)
    }

  object AgNilMonoid extends Monoid[AgNil] {
    override def empty: AgNil = AgNil
    override def combine(h1: AgNil, h2: AgNil): AgNil = AgNil
  }

  def aggConsMonoid[H <: ##[_, _] : Monoid, T <: Aggregation : Monoid]: Monoid[H %:: T] =
    new Monoid[H %:: T] {
      private val headMonoid = Monoid[H]
      private val tailMonoid = Monoid[T]

      override def empty: H %:: T = headMonoid.empty %:: tailMonoid.empty
      override def combine(agg1: H %:: T, agg2: H %:: T): H %:: T =
        headMonoid.combine(agg1.agHead, agg2.agHead) %:: tailMonoid.combine(agg1.agTail, agg2.agTail)
    }

  def aggMonoidImpl[K <: Aggregation : c.WeakTypeTag](c: blackbox.Context): c.Expr[Monoid[K]] = {
    import c.universe._

    val hnil = typeOf[AgNil].dealias
    val K = weakTypeOf[K].dealias

    val expr = K match {
      case `hnil` ⇒ q"AgNilMonoid"
      case _      ⇒ K.typeArgs match {
        case List(head, tail) ⇒ q"aggConsMonoid[$head, $tail]"
        case other            ⇒ throw new IllegalArgumentException(s"Unexpected $K in HList Monoid macro")
      }
    }
    c.Expr[Monoid[K]](expr)
  }

  def arbitraryGroupResultMonoidImpl[
  A: c.WeakTypeTag,
  K <: GroupingCriteria : c.WeakTypeTag,
  T <: Aggregation : c.WeakTypeTag
  ](c: blackbox.Context): c.Expr[Monoid[ArbitraryGroupResult[A, K, T]]] = {

    import c.universe._


    val gnil = typeOf[GNil].dealias
    val A = weakTypeOf[A].dealias
    val T = weakTypeOf[T].dealias
    val K = weakTypeOf[K].dealias

    val Self = tq"ArbitraryGroupResult[$A, $K, $T]"
    val expr = T match {
      case `gnil` ⇒
        println(s"{\n\tA: $A,\n\tT: $T\n\tK: $K\n}")
        throw new IllegalArgumentException(s"T=$T is not allowed")
      case _      ⇒ K.typeArgs match {
        case Nil            ⇒ q"`##@-Monoid`[$A].asInstanceOf[Monoid[$Self]]"
        case List(xKH, xKT) ⇒
          q"""
              new Monoid[$Self] {
                private val totalsMonoid   = Monoid[$T]
                private val mulMonoid      = `~**-Monoid`[$A, $K, $T]
                private val subGroupMonoid = Monoid[ArbitraryGroupResult[$A, $xKT, $T]]

                override def empty: $Self = ~**[$A, $K, $T](totalsMonoid.empty)
                override def combine(x: $Self, y: $Self): $Self = {
                  val newTotals = totalsMonoid.combine(x.totals, y.totals)
                  (x, y) match {
                    case (~::(_, _, xvalues @ _*), ~::(_, _, yvalues @ _*)) if x.sameKey(y)  =>
                      val mergedValues = {
                        val xValuesMap = xvalues.groupBy(_.key)
                        val yValuesMap = yvalues.groupBy(_.key)
                        xValuesMap.mergeConcat(yValuesMap) { (vs1, vs2) =>
                          Vector((vs1 ++ vs2).reduce(subGroupMonoid.combine))
                        }.values.flatten.toSeq
                      }
                      ~::(
                        x.key,
                        newTotals,
                        mergedValues:_*
                      )
                    case (xmul: ~**[_,_,_], ymul: ~**[_,_,_]) =>
                      mulMonoid.combine(xmul, ymul)
                    case _ => ~**(newTotals, x, y).asInstanceOf[$Self]
                  }
                }
              }
            """
      }
    }
    //    println(expr)
    c.Expr[Monoid[ArbitraryGroupResult[A, K, T]]](expr)
  }

  def `~**-Monoid`[A, K <: GroupingCriteria, T <: Aggregation : Monoid]
  (implicit grMonoid: Monoid[ArbitraryGroupResult[A, K, T]]): Monoid[~**[A, K, T]] =
    new Monoid[~**[A, K, T]] {
      override def empty: ~**[A, K, T] = ~**(grMonoid.empty)
      override def combine(
                            x: ~**[A, K, T],
                            y: ~**[A, K, T]
                          ): ~**[A, K, T] = {
        val xValues = x.records.groupBy(_.key)
        val yValues = y.records.groupBy(_.key)
        val merged = xValues.mergeConcat(yValues)(_ ++ _).mapValues(_.reduce(grMonoid.combine))
        ~**(merged.values.toSeq: _*)
      }
    }

  def `##@-Monoid`[A]: Monoid[##@[A]] = new Monoid[##@[A]] {
    override def empty: ##@[A] = ##@()
    override def combine(x: ##@[A], y: ##@[A]): ##@[A] = ##@(x.records ++ y.records: _*)
  }
}