package com.datarootlabs.trembita.ql


import scala.language.{existentials, higherKinds}
import scala.language.experimental.macros
import cats.Monoid
import AggDecl._
import AggRes._
import cats.implicits._
import algebra.ring._


trait aggregationInstances {
  implicit def taggedAggSum[A, U](implicit aMonoid: Monoid[A]): AggFunc[TaggedAgg[A, U, AggFunc.Type.Sum], A ## U, A] =
    new AggFunc[TaggedAgg[A, U, AggFunc.Type.Sum], A ## U, A] {
      def empty: A = aMonoid.empty
      def add(comb: A, value: TaggedAgg[A, U, AggFunc.Type.Sum]): A = comb |+| value.tagged.value
      def combine(c1: A, c2: A): A = c1 |+| c2
      def extract[AA <: TaggedAgg[A, U, AggFunc.Type.Sum], O >: ##[A, U]](comb: A): AggFunc.Result[AA, O, A] =
        AggFunc.Result(comb.as[U], comb)
    }

  implicit def taggedAggCount[A, U]: AggFunc[TaggedAgg[A, U, AggFunc.Type.Count], Long ## U, Long] =
    new AggFunc[TaggedAgg[A, U, AggFunc.Type.Count], Long ## U, Long] {
      def empty: Long = 0
      def add(comb: Long, value: TaggedAgg[A, U, AggFunc.Type.Count]): Long = comb + 1
      def combine(c1: Long, c2: Long): Long = c1 + c2
      def extract[AA <: TaggedAgg[A, U, AggFunc.Type.Count], O >: ##[Long, U]](comb: Long): AggFunc.Result[AA, O, Long] =
        AggFunc.Result(comb.as[U], comb)
    }

  implicit def taggedAggAvg[A, U](implicit FA: Field[A]): AggFunc[TaggedAgg[A, U, AggFunc.Type.Avg], A ## U, (A, BigInt)] =
    new AggFunc[TaggedAgg[A, U, AggFunc.Type.Avg], A ## U, (A, BigInt)] {
      private def b2a(b: BigInt): A = FA.fromBigInt(b)

      def empty: (A, BigInt) = FA.zero → BigInt(0)
      def add(comb: (A, BigInt), value: TaggedAgg[A, U, AggFunc.Type.Avg]): (A, BigInt) =
        (FA.plus(comb._1, value.tagged.value), comb._2 + 1)

      def combine(comb1: (A, BigInt), comb2: (A, BigInt)): (A, BigInt) = (FA.plus(comb1._1, comb2._1), comb1._2 + comb2._2)
      def extract[AA <: TaggedAgg[A, U, AggFunc.Type.Avg], O >: ##[A, U]](comb: (A, BigInt)): AggFunc.Result[AA, O, (A, BigInt)] =
        AggFunc.Result(comb._1.as[U], comb)
    }

  implicit object DNilAggFunc extends AggFunc[DNil, RNil, RNil] {
    def empty: RNil = RNil
    def add(comb: RNil, value: DNil): RNil = RNil
    def combine(comb1: RNil, comb2: RNil): RNil = RNil
    def extract[AA <: DNil, O >: RNil](comb: RNil): AggFunc.Result[AA, O, RNil] = AggFunc.Result(RNil, RNil)
  }

  implicit def aggConsAggFunc[
  A, U, AggF <: AggFunc.Type, AggHComb, HOut <: ##[_, _],
  T <: AggDecl, AggTComb, TOut <: AggRes]
  (implicit AggH: AggFunc[TaggedAgg[A, U, AggF], HOut, AggHComb],
   AggT: AggFunc[T, TOut, AggTComb]
  ): AggFunc[TaggedAgg[A, U, AggF] %:: T, HOut *:: TOut, (AggHComb, AggTComb)] =
    new AggFunc[TaggedAgg[A, U, AggF] %:: T, HOut *:: TOut, (AggHComb, AggTComb)] {

      type H = TaggedAgg[A, U, AggF]

      def empty: (AggHComb, AggTComb) = AggH.empty → AggT.empty

      def add(comb: (AggHComb, AggTComb), value: H %:: T): (AggHComb, AggTComb) =
        (AggH.add(comb._1, value.head), AggT.add(comb._2, value.tail))

      def combine(comb1: (AggHComb, AggTComb), comb2: (AggHComb, AggTComb)): (AggHComb, AggTComb) =
        (AggH.combine(comb1._1, comb2._1), AggT.combine(comb1._2, comb2._2))

      def extract[AA <: TaggedAgg[A, U, AggF] %:: T, O >: *::[HOut, TOut]]
      (comb: (AggHComb, AggTComb)): AggFunc.Result[AA, O, (AggHComb, AggTComb)] = {
        val headRes = AggH.extract(comb._1)
        val tailRes = AggT.extract(comb._2)
        AggFunc.Result(headRes.result *:: tailRes.result, comb)
      }
    }
}