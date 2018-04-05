package com.datarootlabs.trembita.ql


import ArbitraryGroupResult._
import cats.Monoid
import cats.implicits._
import com.datarootlabs.trembita.ql.GroupingCriteria._
import shapeless.DepFn1
import scala.annotation.implicitNotFound


//@implicitNotFound("Cannot convert ArbitraryGroupResult[${A}, ${K}, ${T}] to Map")
//trait ToMap[
//A,
//K <: GroupingCriteria,
//T
//] extends DepFn1[ArbitraryGroupResult[A, K, T]] with Serializable
//
//object ToMap {
//  def apply[A, K <: GroupingCriteria, T]
//  (implicit toTuple: ToMap[A, K, T]): Aux[A, K, T, toTuple.Out] = toTuple
//
//  type Aux[A, K <: GroupingCriteria, T, Out0] = ToMap[A, K, T] {type Out = Out0}
//
//  implicit def lowLevelConsToTuple[A, T]: Aux[A, GNil, T, Seq[A]] =
//    new ToMap[A, GNil, T] {
//      type Out = Seq[A]
//      def apply(t: ArbitraryGroupResult[A, GNil, T]): Out = t match {
//        case recs: ##@[A]         ⇒ recs.records
//        case mul: ~**[A, GNil, T] ⇒ mul.records.collect { case recs: ##@[A] ⇒ recs.records }.flatten
//      }
//    }
//
//  implicit def arbitraryConsToTuple[
//  A,
//  KH <: ##[_, _],
//  KT <: GroupingCriteria,
//  T,
//  SubGroupOut
//  ]
//  (implicit subGroupToTuple: ToMap.Aux[A, KT, T, SubGroupOut]): Aux[A, KH &:: KT, T, Map[KH, (T, SubGroupOut)]] =
//    new ToMap[A, KH &:: KT, T] {
//      type Out = Map[KH, (T, SubGroupOut)]
//      override def apply(t: ArbitraryGroupResult[A, KH &:: KT, T]): Out = t match {
//        case cons: ~::[A, KH, KT, T]   ⇒ Map(cons.key → (cons.totals → subGroupToTuple(cons.subGroup)))
//        case mul: ~**[A, KH &:: KT, T] ⇒ mul.records.map { group ⇒
//          group.key → (group.totals → subGroupToTuple(group.subGroup))
//        }.toMap
//      }
//    }
//}

//@implicitNotFound("Cannot create ArbitraryGroupResult[${A}, ${K}, ${R}] from map")
//abstract class FromMap[
//A,
//K <: GroupingCriteria,
//T, R <: AggRes, AggF <: AggFunc[T, R],
//SubMap
//](implicit aggF: AggF) extends DepFn1[Map[K#Key, (AggF#Comb, SubMap)]] with Serializable
//
//object FromMap {
//  def apply[A,
//  K <: GroupingCriteria,
//  T, R <: AggRes, AggF <: AggFunc[T, R],
//  SubMap]
//  (implicit fromMap: FromMap[A, K, T, R, AggF, SubMap]): Aux[A, K, T, R, AggF, SubMap, fromMap.Out] = fromMap
//
//  type Aux[A,
//  K <: GroupingCriteria,
//  T, R <: AggRes, AggF <: AggFunc[T, R],
//  SubMap, Out0] = FromMap[A, K, T, R, AggF, SubMap] {type Out = Out0}
//
//  implicit def simpleMapToCons
//  [A, GH <: ##[_, _], T, R <: AggRes, AggF <: AggFunc[T, R]]
//  (implicit aggF: AggF)
//  : Aux[A, GH &:: GNil, T, R, AggF, Seq[A], ArbitraryGroupResult[A, GH &:: GNil, R]] =
//    new FromMap[A, GH &:: GNil, T, R, AggF, Seq[A]] {
//      type Out = ArbitraryGroupResult[A, GH &:: GNil, R]
//      def apply(t: Map[GH, (AggF#Comb, Seq[A])]): ArbitraryGroupResult[A, &::[GH, GNil], R] = t.toSeq match {
//        case Seq((key, (comb, recs))) ⇒ ~::(key, aggF.extract(comb.asInstanceOf[aggF.Comb]), ##@(recs: _*))
//        case multiple                 ⇒ ~**(multiple.map {
//          case (key, (comb, recs)) ⇒ ~::(key, aggF.extract(comb.asInstanceOf[aggF.Comb]), ##@(recs: _*))
//        }: _*)
//      }
//    }
//
//  implicit def arbitraryMapToCons[
//  A,
//  KH <: ##[_, _],
//  KT <: GroupingCriteria,
//  T, R <: AggRes, AggF <: AggFunc[T, R],
//  SubMap, SubGroupOut <: ArbitraryGroupResult[A, KT, R]
//  ](
//     implicit subMapToGroupResult: FromMap.Aux[A, KT, T, R, AggF, SubMap, SubGroupOut],
//     aggF: AggF
//   ): Aux[
//    A, KH &:: KT, T, R, AggF, Map[KT#Key, (AggF#Comb, SubMap)],
//    ArbitraryGroupResult[A, KH &:: KT, R]
//    ] =
//    new FromMap[A, KH &:: KT, T, R, AggF, Map[KT#Key, (AggF#Comb, SubMap)]] {
//      type Out = ArbitraryGroupResult[A, KH &:: KT, R]
//      def apply(t: Map[KH, (AggF#Comb, Map[KT#Key, (AggF#Comb, SubMap)])])
//      : ArbitraryGroupResult[A, &::[KH, KT], R] = t.toSeq match {
//        case Seq((key, (comb, subMap))) ⇒ ~::(key, aggF.extract(comb.asInstanceOf[aggF.Comb]), subMapToGroupResult(subMap))
//        case multiple                   ⇒ ~**(multiple.map {
//          case (key, (comb, subMap)) ⇒ ~::(key, aggF.extract(comb.asInstanceOf[aggF.Comb]), subMapToGroupResult(subMap))
//        }: _*)
//      }
//    }
//}
