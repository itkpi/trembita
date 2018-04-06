package com.datarootlabs.trembita.ql


import scala.language.{existentials, higherKinds}
import scala.language.experimental.macros
import cats.Monoid
import scala.reflect.macros.blackbox
import ArbitraryGroupResult._
import GroupingCriteria._
import AggDecl._
import AggRes._
import com.datarootlabs.trembita.utils._
import shapeless._
import cats.implicits._
import algebra.ring._


trait aggregationInstances {
  implicit def taggedAggSum[A, U](implicit aMonoid: Monoid[A]): AggFunc[TaggedAgg[A, U, AggFunc.Type.Sum], A ## U] =
    new AggFunc[TaggedAgg[A, U, AggFunc.Type.Sum], A ## U] {
      type Comb = A
      def empty: A = aMonoid.empty
      def add(comb: A, value: TaggedAgg[A, U, AggFunc.Type.Sum]): A = comb |+| value.tagged.value
      def combine(c1: A, c2: A): A = c1 |+| c2
      def extract[AA <: TaggedAgg[A, U, AggFunc.Type.Sum], O >: ##[A, U]](comb: A): AggFunc.Result[AA, O, A] =
        AggFunc.Result(comb.as[U], comb)
    }

  implicit def taggedAggCount[A, U]: AggFunc[TaggedAgg[A, U, AggFunc.Type.Count], Long ## U] =
    new AggFunc[TaggedAgg[A, U, AggFunc.Type.Count], Long ## U] {
      type Comb = Long
      def empty: Long = 0
      def add(comb: Long, value: TaggedAgg[A, U, AggFunc.Type.Count]): Long = comb + 1
      def combine(c1: Long, c2: Long): Long = c1 + c2
      def extract[AA <: TaggedAgg[A, U, AggFunc.Type.Count], O >: ##[Long, U]](comb: Long): AggFunc.Result[AA, O, Long] =
        AggFunc.Result(comb.as[U], comb)
    }

  implicit def taggedAggAvg[A, U](implicit FA: Field[A]): AggFunc[TaggedAgg[A, U, AggFunc.Type.Avg], A ## U] =
    new AggFunc[TaggedAgg[A, U, AggFunc.Type.Avg], A ## U] {
      type Comb = (A, BigInt)
      private def b2a(b: BigInt): A = FA.fromBigInt(b)

      def empty: (A, BigInt) = FA.zero → BigInt(0)
      def add(comb: (A, BigInt), value: TaggedAgg[A, U, AggFunc.Type.Avg]): (A, BigInt) =
        (FA.plus(comb._1, value.tagged.value), comb._2 + 1)

      def combine(comb1: (A, BigInt), comb2: (A, BigInt)): (A, BigInt) = (FA.plus(comb1._1, comb2._1), comb1._2 + comb2._2)
      def extract[AA <: TaggedAgg[A, U, AggFunc.Type.Avg], O >: ##[A, U]](comb: (A, BigInt)): AggFunc.Result[AA, O, (A, BigInt)] =
        AggFunc.Result(comb._1.as[U], comb)
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
    def empty: HNil = HNil
    def combine(h1: HNil, h2: HNil): HNil = HNil
  }

  def hlistMonoid[H: Monoid, T <: HList : Monoid]: Monoid[H :: T] =
    new Monoid[H :: T] {
      private val headMonoid = Monoid[H]
      private val tailMonoid = Monoid[T]

      def empty: H :: T = headMonoid.empty :: tailMonoid.empty
      def combine(hlist1: H :: T, hlist2: H :: T): H :: T =
        headMonoid.combine(hlist1.head, hlist2.head) :: tailMonoid.combine(hlist1.tail, hlist2.tail)
    }

  implicit object DNilAggFunc extends AggFunc[DNil, RNil] {
    type Comb = RNil
    def empty: RNil = RNil
    def add(comb: RNil, value: DNil): RNil = RNil
    def combine(comb1: RNil, comb2: RNil): RNil = RNil
    def extract[AA <: DNil, O >: RNil](comb: RNil): AggFunc.Result[AA, O, RNil] = AggFunc.Result(RNil, RNil)
  }

  implicit def aggConsAggFunc[
  A, U, AggF <: AggFunc.Type, HOut <: ##[_, _],
  T <: AggDecl, TOut <: AggRes]
  (implicit AggH: AggFunc[TaggedAgg[A, U, AggF], HOut],
   AggT: AggFunc[T, TOut]
  ): AggFunc[TaggedAgg[A, U, AggF] %:: T, HOut *:: TOut] =
    new AggFunc[TaggedAgg[A, U, AggF] %:: T, HOut *:: TOut] {

      type H = TaggedAgg[A, U, AggF]
      type Comb = (AggH.Comb, AggT.Comb)

      def empty: Comb = AggH.empty → AggT.empty

      def add(comb: Comb, value: H %:: T): Comb =
        (AggH.add(comb._1, value.head), AggT.add(comb._2, value.tail))

      def combine(comb1: Comb, comb2: Comb): Comb =
        (AggH.combine(comb1._1, comb2._1), AggT.combine(comb1._2, comb2._2))

      def extract[AA <: TaggedAgg[A, U, AggF] %:: T, O >: *::[HOut, TOut]]
      (comb: (AggH.Comb, AggT.Comb)): AggFunc.Result[AA, O, (AggH.Comb, AggT.Comb)] = {
        val headRes = AggH.extract(comb._1)
        val tailRes = AggT.extract(comb._2)
        AggFunc.Result(headRes.result *:: tailRes.result, comb)
      }
    }

  def arbitraryGroupResultMonoidImpl[
  A: c.WeakTypeTag,
  G <: GroupingCriteria : c.WeakTypeTag,
  T: c.WeakTypeTag
  ](c: blackbox.Context): c.Expr[Monoid[ArbitraryGroupResult[A, G, T]]] = {

    import c.universe._

    val A = weakTypeOf[A].dealias
    val T = weakTypeOf[T].dealias
    val G = weakTypeOf[G].dealias
    val gnil = typeOf[GNil].dealias
    val AggFuncRes = typeOf[AggFunc.Result[_, _, _]].dealias.typeConstructor

    val Self = tq"ArbitraryGroupResult[$A, $G, $T]"

    val expr = G → G.typeArgs match {
      case (`gnil`, _)         ⇒ q"`##@-Monoid`[$A].asInstanceOf[Monoid[$Self]]"
      case (_, List(xGH, xGT)) ⇒
        val tMonoid = T.typeConstructor match {
          case `AggFuncRes` ⇒
            val List(in, r, comb) = T.typeArgs
            q"""
              new Monoid[$T] {
                private val AggF = AggFunc[$in, $r]
                def empty: $T = AggF.extract(AggF.empty).asInstanceOf[$T]
                def combine(x: $T, y: $T): $T = AggF.extract(AggF.combine(
                  x.combiner.asInstanceOf[AggF.Comb],
                  y.combiner.asInstanceOf[AggF.Comb]
                )).asInstanceOf[$T]
              }
            """
          case _            ⇒ q"Monoid[$T]"
        }
        q"""
              new Monoid[$Self] {
                private val tMonoid        = $tMonoid
                private val subGroupMonoid = Monoid[ArbitraryGroupResult[$A, $xGT, $T]]
                private val mulMonoid      = `~**-Monoid`[$A, $G, $T](this, tMonoid.asInstanceOf[Monoid[$T]])

                override def empty: $Self = ~**[$A, $G, $T](tMonoid.empty.asInstanceOf[$T]).asInstanceOf[$Self]
                override def combine(x: $Self, y: $Self): $Self = {
                  (x, y) match {
                    case (~::(k1, xtotals, xSub), ~::(k2, ytotals, ySub)) if k1 == k2 =>
                      ~::[$A, $xGH, $xGT, $T](
                        k1,
                        tMonoid.combine(xtotals, ytotals).asInstanceOf[$T],
                        subGroupMonoid.combine(xSub, ySub)
                      )
                    case (xmul: ~**[_,_,_], ymul: ~**[_,_,_]) =>
                      mulMonoid.combine(
                        xmul.asInstanceOf[~**[$A, $G, $T]],
                        ymul.asInstanceOf[~**[$A, $G, $T]]
                      ).asInstanceOf[$Self]

                    case _ => ~**[$A, $G, $T](tMonoid.combine(x.totals, y.totals).asInstanceOf[$T], x, y).asInstanceOf[$Self]
                  }
                }
              }
            """
    }
    c.Expr[Monoid[ArbitraryGroupResult[A, G, T]]](expr)
  }

  def `~**-Monoid`[A, G <: GroupingCriteria, T]
  (grMonoid: Monoid[ArbitraryGroupResult[A, G, T]], tMonoid: Monoid[T]): Monoid[~**[A, G, T]] =
    new Monoid[~**[A, G, T]] {
      override def empty: ~**[A, G, T] = ~**(tMonoid.empty, grMonoid.empty)
      override def combine(
                            x: ~**[A, G, T],
                            y: ~**[A, G, T]
                          ): ~**[A, G, T] = {
        val xValues = x.records.groupBy(_.key)
        val yValues = y.records.groupBy(_.key)
        val merged = xValues.mergeConcat(yValues)(_ ++ _).mapValues(_.reduce(grMonoid.combine))
        ~**(tMonoid.combine(x.totals, y.totals), merged.values.toSeq: _*)
      }
    }

  def `##@-Monoid`[A]: Monoid[##@[A]] = new Monoid[##@[A]] {
    override def empty: ##@[A] = ##@()
    override def combine(x: ##@[A], y: ##@[A]): ##@[A] = ##@(x.records ++ y.records: _*)
  }
}