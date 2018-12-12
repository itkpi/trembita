package com.github.trembita.ql

import scala.language.{existentials, higherKinds}
import scala.language.experimental.macros
import cats.Monoid
import AggDecl._
import AggRes._
import cats.implicits._
import algebra.ring._
import AggFunc.Type
import spire.algebra.NRoot
import shapeless._
import scala.concurrent.duration._

trait aggregationInstances {

  /**
    * Provides a sum function
    * for type [[A]]
    * having a [[Monoid]] for [[A]]
    *
    * @param aMonoid - Monoid[A]
    * @return - sum function
    **/
  implicit def taggedAggSum[A, U](
    implicit aMonoid: Monoid[A]
  ): AggFunc[TaggedAgg[A, U, AggFunc.Type.Sum], A :@ U, A] =
    new AggFunc[TaggedAgg[A, U, AggFunc.Type.Sum], A :@ U, A] {
      def empty: A = aMonoid.empty
      def add(comb: A, value: TaggedAgg[A, U, AggFunc.Type.Sum]): A =
        comb |+| value.tagged.value
      def combine(c1: A, c2: A): A = c1 |+| c2
      def extract[AA <: TaggedAgg[A, U, AggFunc.Type.Sum], O >: A :@ U](
        comb: A
      ): AggFunc.Result[AA, O, A] =
        AggFunc.Result(comb.as[U], comb)
    }

  /**
    * Provides a counting function
    * for type [[A]]
    *
    * @return - counting function
    **/
  implicit def taggedAggCount[A, U]
    : AggFunc[TaggedAgg[A, U, AggFunc.Type.Count], Long :@ U, Long] =
    new AggFunc[TaggedAgg[A, U, AggFunc.Type.Count], Long :@ U, Long] {
      def empty: Long = 0
      def add(comb: Long, value: TaggedAgg[A, U, AggFunc.Type.Count]): Long =
        comb + 1
      def combine(c1: Long, c2: Long): Long = c1 + c2
      def extract[AA <: TaggedAgg[A, U, AggFunc.Type.Count], O >: Long :@ U](
        comb: Long
      ): AggFunc.Result[AA, O, Long] =
        AggFunc.Result(comb.as[U], comb)
    }

  /**
    * Provides a function that calculates an average value
    * for type [[A]]
    * having a [[Field]] for it
    *
    * @param FA - Field[A]
    * @return - avg function
    **/
  implicit def taggedAggAvg[A, U](
    implicit FA: Field[A]
  ): AggFunc[TaggedAgg[A, U, AggFunc.Type.Avg], A :@ U, A :: BigInt :: HNil] =
    new AggFunc[TaggedAgg[A, U, AggFunc.Type.Avg], A :@ U, A :: BigInt :: HNil] {
      private def b2a(b: BigInt): A = FA.fromBigInt(b)

      def empty: A :: BigInt :: HNil = FA.zero :: BigInt(0) :: HNil
      def add(comb: A :: BigInt :: HNil,
              value: TaggedAgg[A, U, AggFunc.Type.Avg]): A :: BigInt :: HNil = {
        val combValue :: counter :: HNil = comb
        FA.plus(combValue, value.tagged.value) :: (counter + 1) :: HNil
      }

      def combine(comb1: A :: BigInt :: HNil,
                  comb2: A :: BigInt :: HNil): A :: BigInt :: HNil = {
        val combValue1 :: counter1 :: HNil = comb1
        val combValue2 :: counter2 :: HNil = comb2
        FA.plus(combValue1, combValue2) :: (counter1 + counter2) :: HNil
      }

      def extract[AA <: TaggedAgg[A, U, AggFunc.Type.Avg], O >: A :@ U](
        comb: A :: BigInt :: HNil
      ): AggFunc.Result[AA, O, A :: BigInt :: HNil] = {
        val combValue :: counter :: HNil = comb
        AggFunc.Result(FA.div(combValue, FA.fromBigInt(counter)).as[U], comb)
      }
    }

  /**
    * Provides a function getting maximal value
    * for type [[A]]
    * having an [[Ordering]] for [[A]]
    *
    * @param Ord - ordering for [[A]]
    * @return - max function
    **/
  implicit def taggedAggMax[A, U](
    implicit Ord: Ordering[A],
    D: Default[A]
  ): AggFunc[TaggedAgg[A, U, AggFunc.Type.Max], A :@ U, A] =
    new AggFunc[TaggedAgg[A, U, AggFunc.Type.Max], A :@ U, A] {

      def empty: A = D.get
      def add(comb: A, value: TaggedAgg[A, U, AggFunc.Type.Max]): A =
        Ord.max(comb, value.tagged.value)

      def combine(comb1: A, comb2: A): A = Ord.max(comb1, comb2)
      def extract[AA <: TaggedAgg[A, U, AggFunc.Type.Max], O >: A :@ U](
        comb: A
      ): AggFunc.Result[AA, O, A] =
        AggFunc.Result(comb.as[U], comb)
    }

  /**
    * Provides a function getting minimal value
    * for type [[A]]
    * having an [[Ordering]] for [[A]]
    *
    * @param Ord - ordering for [[A]]
    * @return - min function
    **/
  implicit def taggedAggMin[A, U](
    implicit Ord: Ordering[A],
    D: Default[A]
  ): AggFunc[TaggedAgg[A, U, AggFunc.Type.Min], A :@ U, A] =
    new AggFunc[TaggedAgg[A, U, AggFunc.Type.Min], A :@ U, A] {

      def empty: A = D.get
      def add(comb: A, value: TaggedAgg[A, U, AggFunc.Type.Min]): A =
        Ord.min(comb, value.tagged.value)

      def combine(comb1: A, comb2: A): A = Ord.min(comb1, comb2)
      def extract[AA <: TaggedAgg[A, U, AggFunc.Type.Min], O >: A :@ U](
        comb: A
      ): AggFunc.Result[AA, O, A] =
        AggFunc.Result(comb.as[U], comb)
    }

  /**
    * Provides a multiplication function
    * for type [[A]]
    * having a [[Rng]] for [[A]]
    *
    * @param Rng - Rng[A]
    * @return - multiplication function
    **/
  implicit def taggedAggProduct[A, U](
    implicit Rng: Rng[A]
  ): AggFunc[TaggedAgg[A, U, AggFunc.Type.Product], A :@ U, A] =
    new AggFunc[TaggedAgg[A, U, AggFunc.Type.Product], A :@ U, A] {

      def empty: A = Rng.zero
      def add(comb: A, value: TaggedAgg[A, U, AggFunc.Type.Product]): A =
        Rng.times(comb, value.tagged.value)

      def combine(comb1: A, comb2: A): A = Rng.times(comb1, comb2)
      def extract[AA <: TaggedAgg[A, U, AggFunc.Type.Product], O >: A :@ U](
        comb: A
      ): AggFunc.Result[AA, O, A] =
        AggFunc.Result(comb.as[U], comb)
    }

  /**
    * Provides a function
    * collecting all values of type [[A]]
    * into a [[Vector]]
    *
    * @return - collecting function
    **/
  implicit def taggedAggArr[A, U]
    : AggFunc[TaggedAgg[A, U, AggFunc.Type.Arr], Vector[A] :@ U, Vector[A]] =
    new AggFunc[TaggedAgg[A, U, AggFunc.Type.Arr], Vector[A] :@ U, Vector[A]] {

      def empty: Vector[A] = Vector.empty
      def add(comb: Vector[A],
              value: TaggedAgg[A, U, AggFunc.Type.Arr]): Vector[A] =
        comb :+ value.tagged.value

      def combine(comb1: Vector[A], comb2: Vector[A]): Vector[A] =
        comb1 ++ comb2
      def extract[AA <: TaggedAgg[A, U, Type.Arr], O >: :@[Vector[A], U]](
        comb: Vector[A]
      ): AggFunc.Result[AA, O, Vector[A]] =
        AggFunc.Result(comb.:@[U], comb)
    }

  /**
    * Provides a function
    * concatenating string representations for each [[A]]
    *
    * @return - a function evaluating concatenated string representations
    **/
  implicit def taggedAggStringAgg[A, U]
    : AggFunc[TaggedAgg[A, U, AggFunc.Type.StringAgg],
              String :@ U,
              StringBuilder] =
    new AggFunc[TaggedAgg[A, U, AggFunc.Type.StringAgg], String :@ U, StringBuilder] {

      def empty: StringBuilder = new StringBuilder
      def add(comb: StringBuilder,
              value: TaggedAgg[A, U, AggFunc.Type.StringAgg]): StringBuilder =
        comb.append(value.tagged.value.toString)

      def combine(comb1: StringBuilder, comb2: StringBuilder): StringBuilder =
        comb1.append(comb2)
      def extract[AA <: TaggedAgg[A, U, Type.StringAgg], O >: String :@ U](
        comb: StringBuilder
      ): AggFunc.Result[AA, O, StringBuilder] =
        AggFunc.Result(comb.toString.:@[U], comb)
    }

  /**
    * Provides a function that calculates standard deviation
    * for type [[A]]
    * having a [[Field]] and [[NRoot]] for it
    *
    * @param FA    - Field[A]
    * @param NRoot - NRoot[A]
    * @return - STDEV function
    **/
  implicit def taggedAggStandardDeviation[A, U](implicit FA: Field[A],
                                                NRoot: NRoot[A]): AggFunc[TaggedAgg[
    A,
    U,
    AggFunc.Type.STDEV
  ], A :@ U, A :: Vector[A] :: BigInt :: HNil] =
    new AggFunc[TaggedAgg[A, U, AggFunc.Type.STDEV], A :@ U, A :: Vector[A] :: BigInt :: HNil] {

      def empty: A :: Vector[A] :: BigInt :: HNil =
        FA.zero :: Vector.empty[A] :: BigInt(0) :: HNil
      def add(
        comb: A :: Vector[A] :: BigInt :: HNil,
        value: TaggedAgg[A, U, AggFunc.Type.STDEV]
      ): A :: Vector[A] :: BigInt :: HNil = {
        val combValue :: vs :: counter :: HNil = comb
        FA.plus(combValue, value.tagged.value) :: (vs :+ value.tagged.value) :: (counter + 1) :: HNil
      }

      def combine(
        comb1: A :: Vector[A] :: BigInt :: HNil,
        comb2: A :: Vector[A] :: BigInt :: HNil
      ): A :: Vector[A] :: BigInt :: HNil = {
        val combValue1 :: vs1 :: counter1 :: HNil = comb1
        val combValue2 :: vs2 :: counter2 :: HNil = comb2
        FA.plus(combValue1, combValue2) :: (vs1 ++ vs2) :: (counter1 + counter2) :: HNil
      }

      def extract[AA <: TaggedAgg[A, U, Type.STDEV], O >: A :@ U](
        comb: A :: Vector[A] :: BigInt :: HNil
      ): AggFunc.Result[AA, O, A :: Vector[A] :: BigInt :: HNil] = {
        val sum :: xs :: count :: HNil = comb
        val countA = FA.fromBigInt(count)
        val avgX = FA.div(sum, countA)
        val σ = NRoot.sqrt(
          FA.div(
            xs.map(x => FA.pow(FA.minus(x, avgX), 2))
              .foldLeft(FA.zero)(FA.plus),
            countA
          )
        )
        AggFunc.Result(σ.:@[U], comb)
      }
    }

  /**
    * Provides a function that calculates root mean square
    * for type [[A]]
    * having a [[Field]] and [[NRoot]] for it
    *
    * @param FA    - Field[A]
    * @param NRoot - NRoot[A]
    * @return - STDEV function
    **/
  implicit def taggedAggRootMeanSquare[A, U](
    implicit FA: Field[A],
    NRoot: NRoot[A]
  ): AggFunc[TaggedAgg[A, U, AggFunc.Type.RMS], A :@ U, A :: BigInt :: HNil] =
    new AggFunc[TaggedAgg[A, U, AggFunc.Type.RMS], A :@ U, A :: BigInt :: HNil] {

      def empty: A :: BigInt :: HNil = FA.zero :: BigInt(0) :: HNil
      def add(comb: A :: BigInt :: HNil,
              value: TaggedAgg[A, U, AggFunc.Type.RMS]): A :: BigInt :: HNil = {
        val sum :: counter :: HNil = comb
        FA.plus(sum, FA.pow(value.tagged.value, 2)) :: (counter + 1) :: HNil
      }

      def combine(comb1: A :: BigInt :: HNil,
                  comb2: A :: BigInt :: HNil): A :: BigInt :: HNil = {
        val sum1 :: counter1 :: HNil = comb1
        val sum2 :: counter2 :: HNil = comb2
        FA.plus(sum1, sum2) :: (counter1 + counter2) :: HNil
      }

      def extract[AA <: TaggedAgg[A, U, Type.RMS], O >: A :@ U](
        comb: A :: BigInt :: HNil
      ): AggFunc.Result[AA, O, A :: BigInt :: HNil] = {
        val sumOfSquares :: count :: HNil = comb
        val countA = FA.fromBigInt(count)
        AggFunc.Result(NRoot.sqrt(FA.div(sumOfSquares, countA)).:@[U], comb)
      }
    }

  /**
    * Provides a function
    * getting a random value of type [[A]]
    * having a default value for a fallback
    *
    * @param D - instance of [[Default]]
    * @return - function getting random value
    **/
  implicit def taggedAggRandom[A, U](
    implicit D: Default[A]
  ): AggFunc[TaggedAgg[A, U, AggFunc.Type.Random], A :@ U, A] =
    new AggFunc[TaggedAgg[A, U, AggFunc.Type.Random], A :@ U, A] {
      private val rnd = new java.security.SecureRandom()
      def empty: A = D.get
      def add(comb: A, value: TaggedAgg[A, U, AggFunc.Type.Random]): A =
        List(comb, value.tagged.value)(rnd.nextInt(1))

      def combine(comb1: A, comb2: A): A = List(comb1, comb2)(rnd.nextInt(1))
      def extract[AA <: TaggedAgg[A, U, AggFunc.Type.Random], O >: A :@ U](
        comb: A
      ): AggFunc.Result[AA, O, A] =
        AggFunc.Result(comb.as[U], comb)
    }

  /** [[AggFunc]] for [[DNil]] and [[RNil]] */
  implicit object DNilAggFunc extends AggFunc[DNil, RNil, RNil] {
    def empty: RNil = RNil
    def add(comb: RNil, value: DNil): RNil = RNil
    def combine(comb1: RNil, comb2: RNil): RNil = RNil
    def extract[AA <: DNil, O >: RNil](
      comb: RNil
    ): AggFunc.Result[AA, O, RNil] = AggFunc.Result(RNil, RNil)
  }

  /**
    * Creates an [[AggFunc]]
    * for an arbitrary aggregation declarations
    *
    * @param AggH - aggregation function for the first declaration
    * @param AggT - aggregation function for the rest of declarations
    * @return - an arbitrary aggregation function
    **/
  implicit def aggConsAggFunc[A,
                              U,
                              AggF <: AggFunc.Type,
                              AggHComb,
                              HOut <: :@[_, _],
                              T <: AggDecl,
                              AggTComb,
                              TOut <: AggRes](
    implicit AggH: AggFunc[TaggedAgg[A, U, AggF], HOut, AggHComb],
    AggT: AggFunc[T, TOut, AggTComb]
  ): AggFunc[TaggedAgg[A, U, AggF] %:: T,
             HOut *:: TOut,
             AggHComb :: AggTComb :: HNil] =
    new AggFunc[TaggedAgg[A, U, AggF] %:: T, HOut *:: TOut, AggHComb :: AggTComb :: HNil] {

      type H = TaggedAgg[A, U, AggF]

      def empty: AggHComb :: AggTComb :: HNil = AggH.empty :: AggT.empty :: HNil

      def add(comb: AggHComb :: AggTComb :: HNil,
              value: H %:: T): AggHComb :: AggTComb :: HNil =
        AggH.add(comb(0), value.head) :: AggT.add(comb(1), value.tail) :: HNil

      def combine(
        comb1: AggHComb :: AggTComb :: HNil,
        comb2: AggHComb :: AggTComb :: HNil
      ): AggHComb :: AggTComb :: HNil =
        AggH.combine(comb1(0), comb2(0)) :: AggT.combine(comb1(1), comb2(1)) :: HNil

      def extract[AA <: TaggedAgg[A, U, AggF] %:: T, O >: *::[HOut, TOut]](
        comb: AggHComb :: AggTComb :: HNil
      ): AggFunc.Result[AA, O, AggHComb :: AggTComb :: HNil] = {
        val headRes = AggH.extract(comb(0))
        val tailRes = AggT.extract(comb(1))
        AggFunc.Result(headRes.result *:: tailRes.result, comb)
      }
    }

  implicit object FiniteDurationAlgrebra extends Field[FiniteDuration] {
    def negate(x: FiniteDuration): FiniteDuration = -x
    def zero: FiniteDuration = Duration.Zero
    def plus(x: FiniteDuration, y: FiniteDuration): FiniteDuration = x + y
    def div(x: FiniteDuration, y: FiniteDuration): FiniteDuration = {
      val ynanos = y.toNanos
      if (ynanos == 0) 0.nanos
      else (x.toNanos / ynanos).nanos
    }
    def one: FiniteDuration = 1.nano
    def times(x: FiniteDuration, y: FiniteDuration): FiniteDuration =
      (x.toNanos * y.toNanos).nanos
  }
}
