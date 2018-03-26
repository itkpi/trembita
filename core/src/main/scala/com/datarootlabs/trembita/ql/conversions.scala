package com.datarootlabs.trembita.ql


import ArbitraryGroupResult._
import cats.Monoid
import cats.implicits._
import com.datarootlabs.trembita.ql.GroupingCriteria._
import shapeless.DepFn1
import scala.annotation.implicitNotFound


@implicitNotFound("Cannot convert ArbitraryGroupResult[${A}, ${K}, ${T}] to Map")
trait ToMap[
A,
K <: GroupingCriteria,
T <: Aggregation
] extends DepFn1[ArbitraryGroupResult[A, K, T]] with Serializable

object ToMap {
  def apply[A, K <: GroupingCriteria, T <: Aggregation]
  (implicit toTuple: ToMap[A, K, T]): Aux[A, K, T, toTuple.Out] = toTuple

  type Aux[A, K <: GroupingCriteria, T <: Aggregation, Out0] = ToMap[A, K, T] {type Out = Out0}

  implicit def lowLevelConsToTuple[A, T <: Aggregation]: Aux[A, GNil, T, Seq[A]] =
    new ToMap[A, GNil, T] {
      type Out = Seq[A]
      def apply(t: ArbitraryGroupResult[A, GNil, T]): Out = t match {
        case recs: ##@[A]         ⇒ recs.records
        case mul: ~**[A, GNil, T] ⇒ mul.records.collect { case recs: ##@[A] ⇒ recs.records }.flatten
      }
    }

  implicit def arbitraryConsToTuple[
  A,
  KH <: ##[_, _],
  KT <: GroupingCriteria,
  T <: Aggregation,
  SubGroupOut
  ]
  (implicit subGroupToTuple: ToMap.Aux[A, KT, T, SubGroupOut]): Aux[A, KH &:: KT, T, Map[KH, (T, SubGroupOut)]] =
    new ToMap[A, KH &:: KT, T] {
      type Out = Map[KH, (T, SubGroupOut)]
      override def apply(t: ArbitraryGroupResult[A, KH &:: KT, T]): Out = t match {
        case cons: ~::[A, KH, KT, T]   ⇒ Map(cons.key → (cons.totals → subGroupToTuple(cons.subGroup)))
        case mul: ~**[A, KH &:: KT, T] ⇒ mul.records.map { group ⇒
          group.key → (group.totals → subGroupToTuple(group.subGroup))
        }.toMap
      }
    }
}

@implicitNotFound("Cannot create ArbitraryGroupResult[${A}, ${K}, ${T}] from map")
trait FromMap[
A,
K <: GroupingCriteria,
T <: Aggregation
] extends DepFn1[ToMap[A, K, T]#Out] with Serializable

object FromMap {
  def apply[A, K <: GroupingCriteria, T <: Aggregation, SubGrMap]
  (implicit fromMap: FromMap[A, K, T]): Aux[A, K, T, fromMap.Out] = fromMap

  type Aux[A, K <: GroupingCriteria, T <: Aggregation, Out0] = FromMap[A, K, T] {type Out = Out0}

  implicit def simpleMapToCons[A, T <: Aggregation]: Aux[
    A, GNil, T,
    ArbitraryGroupResult[A, GNil, T]
    ] =
    new FromMap[A, GNil, T] {
      type Out = ArbitraryGroupResult[A, GNil, T]
      def apply(t: ToMap[A, GNil, T]#Out): ArbitraryGroupResult[A, GNil, T] = t match {
        case seq: Seq[A] ⇒ ##@[A](seq: _*)
      }
    }

  implicit def arbitraryMapToCons[
  A,
  KH <: ##[_, _],
  KT <: GroupingCriteria,
  T <: Aggregation : Monoid,
  SubGroupOut
  ](
     implicit subMapToGroupResult: FromMap.Aux[A, KT, T, SubGroupOut]
   ): Aux[
    A, KH &:: KT, T,
    ArbitraryGroupResult[A, KH &:: KT, T]
    ] =
    new FromMap[A, KH &:: KT, T] {
      type Out = ArbitraryGroupResult[A, KH &:: KT, T]
      def apply(t: ToMap[A, KH &:: KT, T]#Out): ArbitraryGroupResult[A, KH &:: KT, T] = t match {
        case m: Map[KH, (T, ToMap[A, KT, T]#Out)] ⇒ m.toSeq match {
          case Seq((key, (totals, tailOut))) ⇒
            ~::(key, totals, subMapToGroupResult(tailOut).asInstanceOf[ArbitraryGroupResult[A, KT, T]])

          case multiple ⇒ ~**(multiple.map { case (key, (totals, tailOut)) ⇒
            ~::(key, totals, subMapToGroupResult(tailOut).asInstanceOf[ArbitraryGroupResult[A, KT, T]])
          }: _*)
        }
      }
    }
}
