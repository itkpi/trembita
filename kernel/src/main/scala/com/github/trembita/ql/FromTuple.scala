package com.github.trembita.ql

import shapeless._
import GroupingCriteria._
import AggDecl._
import AggRes._

trait FromTuple[T] extends DepFn1[T]
sealed trait LowPriorityQl {
  implicit def self[A]: FromTuple.Aux[A, A] = new FromTuple[A] {
    type Out = A
    def apply(t: A): A = t
  }
}

sealed trait GroupingCriteriaFromTuple {
  implicit def singleGroupingCriteria[A <: :@[_, _]]
    : FromTuple.Aux[A, A &:: GNil] = new FromTuple[A] {
    type Out = A &:: GNil
    def apply(t: A): A &:: GNil = t &:: GNil
  }
  implicit def tuple2GroupingCriteria[A <: :@[_, _], B <: :@[_, _]]
    : FromTuple.Aux[(A, B), A &:: B &:: GNil] = new FromTuple[(A, B)] {
    type Out = A &:: B &:: GNil
    def apply(t: (A, B)): A &:: B &:: GNil = t._1 &:: t._2 &:: GNil
  }
  implicit def tuple3GroupingCriteria[A <: :@[_, _], B <: :@[_, _], C <: :@[_,
                                                                            _]]
    : FromTuple.Aux[(A, B, C), A &:: B &:: C &:: GNil] =
    new FromTuple[(A, B, C)] {
      type Out = A &:: B &:: C &:: GNil
      def apply(t: (A, B, C)): Out = t._1 &:: t._2 &:: t._3 &:: GNil
    }
  implicit def tuple4GroupingCriteria[A <: :@[_, _],
                                      B <: :@[_, _],
                                      C <: :@[_, _],
                                      D <: :@[_, _]]
    : FromTuple.Aux[(A, B, C, D), A &:: B &:: C &:: D &:: GNil] =
    new FromTuple[(A, B, C, D)] {
      type Out = A &:: B &:: C &:: D &:: GNil
      def apply(t: (A, B, C, D)): Out = t._1 &:: t._2 &:: t._3 &:: t._4 &:: GNil
    }
  implicit def tuple5GroupingCriteria[A <: :@[_, _],
                                      B <: :@[_, _],
                                      C <: :@[_, _],
                                      D <: :@[_, _],
                                      E <: :@[_, _]]
    : FromTuple.Aux[(A, B, C, D, E), A &:: B &:: C &:: D &:: E &:: GNil] =
    new FromTuple[(A, B, C, D, E)] {
      type Out = A &:: B &:: C &:: D &:: E &:: GNil
      def apply(t: (A, B, C, D, E)): Out =
        t._1 &:: t._2 &:: t._3 &:: t._4 &:: t._5 &:: GNil
    }

  implicit def tuple6GroupingCriteria[A <: :@[_, _],
                                      B <: :@[_, _],
                                      C <: :@[_, _],
                                      D <: :@[_, _],
                                      E <: :@[_, _],
                                      F <: :@[_, _]]
    : FromTuple.Aux[(A, B, C, D, E, F),
                    A &:: B &:: C &:: D &:: E &:: F &:: GNil] =
    new FromTuple[(A, B, C, D, E, F)] {
      type Out = A &:: B &:: C &:: D &:: E &:: F &:: GNil
      def apply(t: (A, B, C, D, E, F)): Out =
        t._1 &:: t._2 &:: t._3 &:: t._4 &:: t._5 &:: t._6 &:: GNil
    }

  implicit def tuple7GroupingCriteria[A <: :@[_, _],
                                      B <: :@[_, _],
                                      C <: :@[_, _],
                                      D <: :@[_, _],
                                      E <: :@[_, _],
                                      F <: :@[_, _],
                                      G <: :@[_, _]]
    : FromTuple.Aux[(A, B, C, D, E, F, G),
                    A &:: B &:: C &:: D &:: E &:: F &:: G &:: GNil] =
    new FromTuple[(A, B, C, D, E, F, G)] {
      type Out = A &:: B &:: C &:: D &:: E &:: F &:: G &:: GNil
      def apply(t: (A, B, C, D, E, F, G)): Out =
        t._1 &:: t._2 &:: t._3 &:: t._4 &:: t._5 &:: t._6 &:: t._7 &:: GNil
    }
}

sealed trait AggDeclFromTuple {
  implicit def singleaggDecl[A <: TaggedAgg[_, _, _]]
    : FromTuple.Aux[A, A %:: DNil] =
    new FromTuple[A] {
      type Out = A %:: DNil
      def apply(t: A): A %:: DNil = t %:: DNil
    }
  implicit def tuple2aggDecl[A <: TaggedAgg[_, _, _], B <: TaggedAgg[_, _, _]]
    : FromTuple.Aux[(A, B), A %:: B %:: DNil] = new FromTuple[(A, B)] {
    type Out = A %:: B %:: DNil
    def apply(t: (A, B)): A %:: B %:: DNil = t._1 %:: t._2 %:: DNil
  }
  implicit def tuple3aggDecl[A <: TaggedAgg[_, _, _],
                             B <: TaggedAgg[_, _, _],
                             C <: TaggedAgg[_, _, _]]
    : FromTuple.Aux[(A, B, C), A %:: B %:: C %:: DNil] =
    new FromTuple[(A, B, C)] {
      type Out = A %:: B %:: C %:: DNil
      def apply(t: (A, B, C)): Out = t._1 %:: t._2 %:: t._3 %:: DNil
    }
  implicit def tuple4aggDecl[A <: TaggedAgg[_, _, _],
                             B <: TaggedAgg[_, _, _],
                             C <: TaggedAgg[_, _, _],
                             D <: TaggedAgg[_, _, _]]
    : FromTuple.Aux[(A, B, C, D), A %:: B %:: C %:: D %:: DNil] =
    new FromTuple[(A, B, C, D)] {
      type Out = A %:: B %:: C %:: D %:: DNil
      def apply(t: (A, B, C, D)): Out = t._1 %:: t._2 %:: t._3 %:: t._4 %:: DNil
    }
  implicit def tuple5aggDecl[A <: TaggedAgg[_, _, _],
                             B <: TaggedAgg[_, _, _],
                             C <: TaggedAgg[_, _, _],
                             D <: TaggedAgg[_, _, _],
                             E <: TaggedAgg[_, _, _]]
    : FromTuple.Aux[(A, B, C, D, E), A %:: B %:: C %:: D %:: E %:: DNil] =
    new FromTuple[(A, B, C, D, E)] {
      type Out = A %:: B %:: C %:: D %:: E %:: DNil
      def apply(t: (A, B, C, D, E)): Out =
        t._1 %:: t._2 %:: t._3 %:: t._4 %:: t._5 %:: DNil
    }

  implicit def tuple6aggDecl[A <: TaggedAgg[_, _, _],
                             B <: TaggedAgg[_, _, _],
                             C <: TaggedAgg[_, _, _],
                             D <: TaggedAgg[_, _, _],
                             E <: TaggedAgg[_, _, _],
                             F <: TaggedAgg[_, _, _]]
    : FromTuple.Aux[(A, B, C, D, E, F),
                    A %:: B %:: C %:: D %:: E %:: F %:: DNil] =
    new FromTuple[(A, B, C, D, E, F)] {
      type Out = A %:: B %:: C %:: D %:: E %:: F %:: DNil
      def apply(t: (A, B, C, D, E, F)): Out =
        t._1 %:: t._2 %:: t._3 %:: t._4 %:: t._5 %:: t._6 %:: DNil
    }

  implicit def tuple7aggDecl[A <: TaggedAgg[_, _, _],
                             B <: TaggedAgg[_, _, _],
                             C <: TaggedAgg[_, _, _],
                             D <: TaggedAgg[_, _, _],
                             E <: TaggedAgg[_, _, _],
                             F <: TaggedAgg[_, _, _],
                             G <: TaggedAgg[_, _, _]]
    : FromTuple.Aux[(A, B, C, D, E, F, G),
                    A %:: B %:: C %:: D %:: E %:: F %:: G %:: DNil] =
    new FromTuple[(A, B, C, D, E, F, G)] {
      type Out = A %:: B %:: C %:: D %:: E %:: F %:: G %:: DNil
      def apply(t: (A, B, C, D, E, F, G)): Out =
        t._1 %:: t._2 %:: t._3 %:: t._4 %:: t._5 %:: t._6 %:: t._7 %:: DNil
    }
}

object FromTuple extends GroupingCriteriaFromTuple with AggDeclFromTuple with LowPriorityQl {
  type Aux[T, Out0] = FromTuple[T] { type Out = Out0 }
}
