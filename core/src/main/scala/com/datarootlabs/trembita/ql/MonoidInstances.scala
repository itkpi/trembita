package com.datarootlabs.trembita.ql


import scala.language.{existentials, higherKinds}
import scala.language.experimental.macros
import cats.Monoid
import shapeless._
import scala.reflect.macros.blackbox
import ArbitraryGroupResult._
import GroupingCriteria._
import AggDecl._
import AggRes._
import com.datarootlabs.trembita.utils._
import shapeless._
import cats.implicits._
import algebra.ring._


trait MonoidInstances {
  implicit def taggedAggSum[A, U](implicit aMonoid: Monoid[A]): AggFunc[TaggedAgg[A, U, AggFunc.Type.Sum], A ## U] =
    new AggFunc[TaggedAgg[A, U, AggFunc.Type.Sum], A ## U] {
      type Comb = A
      def empty: A = aMonoid.empty
      def add(comb: A, value: TaggedAgg[A, U, AggFunc.Type.Sum]): A = comb |+| value.tagged.value
      def combine(c1: A, c2: A): A = c1 |+| c2
      override def extract[O >: ##[A, U]](comb: A): AggFunc.Result[O, A] = AggFunc.Result(comb.as[U], comb)
    }

  implicit def taggedAggCount[A, U]: AggFunc[TaggedAgg[A, U, AggFunc.Type.Count], Long ## U] =
    new AggFunc[TaggedAgg[A, U, AggFunc.Type.Count], Long ## U] {
      type Comb = Long
      def empty: Long = 0
      def add(comb: Long, value: TaggedAgg[A, U, AggFunc.Type.Count]): Long = comb + 1
      def combine(c1: Long, c2: Long): Long = c1 + c2
      def extract[O >: ##[Long, U]](comb: Long): AggFunc.Result[O, Long] = AggFunc.Result(comb.as[U], comb)
    }

  implicit def taggedAggAvg[A, U](implicit FA: Field[A]): AggFunc[TaggedAgg[A, U, AggFunc.Type.Avg], A ## U] =
    new AggFunc[TaggedAgg[A, U, AggFunc.Type.Avg], A ## U] {
      type Comb = (A, BigInt)
      private def b2a(b: BigInt): A = FA.fromBigInt(b)

      def empty: (A, BigInt) = FA.zero → BigInt(0)
      def add(comb: (A, BigInt), value: TaggedAgg[A, U, AggFunc.Type.Avg]): (A, BigInt) =
        (FA.plus(comb._1, value.tagged.value), comb._2 + 1)

      def combine(comb1: (A, BigInt), comb2: (A, BigInt)): (A, BigInt) = (FA.plus(comb1._1, comb2._1), comb1._2 + comb2._2)
       def extract[O >: ##[A, U]](comb: (A, BigInt)): AggFunc.Result[O, (A, BigInt)] = AggFunc.Result(comb._1.as[U], comb)
    }

  //  def taggedAgg[A, U, AggT <: AggFunc.Type](c: blackbox.Context): c.Expr[AggFunc[TaggedAgg[A, U, AggT]]]

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
     def extract[O >: RNil](comb: RNil): AggFunc.Result[O, RNil] = AggFunc.Result(RNil, RNil)
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

       def extract[O >: *::[HOut, TOut]](comb: (AggH.Comb, AggT.Comb)): AggFunc.Result[O, (AggH.Comb, AggT.Comb)] = {
         val headRes = AggH.extract(comb._1)
         val tailRes = AggT.extract(comb._2)
         AggFunc.Result(headRes.result *:: tailRes.result, comb)
       }
    }


  /*
    def aggMonoidImpl[T <: Aggregation : c.WeakTypeTag](c: blackbox.Context): c.Expr[AggFunc[T]] = {
      import c.universe._

      val hnil = typeOf[AgNil].dealias
      val T = weakTypeOf[T].dealias

      val expr = T match {
        case `hnil` ⇒ q"AgNilAggFunc"
        case _      ⇒ T.typeArgs match {
          case List(head, tail) ⇒ q"aggConsAggFunc[$head, $tail]"
          case other            ⇒ throw new IllegalArgumentException(s"Unexpected $T in Aggregation AggFunc macro")
        }
      }
      c.Expr[AggFunc[T]](expr)
    }
  */

  def arbitraryGroupResultMonoidImpl[
  A: c.WeakTypeTag,
  K <: GroupingCriteria : c.WeakTypeTag,
  T : c.WeakTypeTag
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
                private val aggT           = AggFunc[$T]
                private val mulMonoid      = `~**-Monoid`[$A, $K, $T]
                private val subGroupMonoid = Monoid[ArbitraryGroupResult[$A, $xKT, $T]]

                override def empty: $Self = ~**[$A, $K, $T]()
                override def combine(x: $Self, y: $Self): $Self = {
                  val newTotals = aggT.combine(x.totals, y.totals)
                  (x, y) match {
                    case (~::(_, _, xvalues @ _*), ~::(_, _, yvalues @ _*)) if x.sameKey(y)  =>
                      val mergedValues = {
                        val xValuesMap = xvalues.groupBy(_.key)
                        val yValuesMap = yvalues.groupBy(_.key)
                        xValuesMap.mergeConcat(yValuesMap) { (vs1, vs2) =>
                          Vector((vs1 ++ vs2).reduce(aggT.combine))
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

  def `~**-Monoid`[A, K <: GroupingCriteria, T  : Monoid]
  (implicit grMonoid: Monoid[ArbitraryGroupResult[A, K, T]]): Monoid[~**[A, K, T]] =
    new Monoid[~**[A, K, T]] {
      override def empty: ~**[A, K, T] = ~**(Monoid[T].empty, grMonoid.empty)
      override def combine(
                            x: ~**[A, K, T],
                            y: ~**[A, K, T]
                          ): ~**[A, K, T] = {
        val xValues = x.records.groupBy(_.key)
        val yValues = y.records.groupBy(_.key)
        val merged = xValues.mergeConcat(yValues)(_ ++ _).mapValues(_.reduce(grMonoid.combine))
        ~**(x.totals |+| y.totals, merged.values.toSeq: _*)
      }
    }

  def `##@-Monoid`[A]: Monoid[##@[A]] = new Monoid[##@[A]] {
    override def empty: ##@[A] = ##@()
    override def combine(x: ##@[A], y: ##@[A]): ##@[A] = ##@(x.records ++ y.records: _*)
  }
}