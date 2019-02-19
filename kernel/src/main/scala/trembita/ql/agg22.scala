package trembita.ql

import trembita.Environment
import trembita.ql.AggDecl.{%::, DNil}
import trembita.ql.QueryBuilder.{Aggregate, GroupBy}
import scala.language.higherKinds

trait agg22[F[_], Er, A, E <: Environment, G <: GroupingCriteria] { self: GroupBy[F, Er, A, E, G] =>

  /**
    * Like Group By clause in SQL
    *
    **/
  def aggregate[T, H <: TaggedAgg[_, _, _], R <: AggRes, Comb](
      f: A => H
  )(implicit aggF: AggFunc[H %:: DNil, R, Comb]): Aggregate[F, Er, A, E, G, H %:: DNil, R, Comb] =
    new Aggregate[F, Er, A, E, G, H %:: DNil, R, Comb](pipeline, self.getG, a => f(a) %:: DNil, self.filterOpt)

  def aggregate[T, H1 <: TaggedAgg[_, _, _], H2 <: TaggedAgg[_, _, _], R <: AggRes, Comb](
      f1: A => H1,
      f2: A => H2
  )(implicit aggF: AggFunc[
      H1 %::
        H2 %::
        DNil,
      R,
      Comb
    ]): Aggregate[F, Er, A, E, G, H1 %:: H2 %:: DNil, R, Comb] =
    new Aggregate[F, Er, A, E, G, H1 %:: H2 %:: DNil, R, Comb](pipeline, self.getG, a => f1(a) %:: f2(a) %:: DNil, self.filterOpt)

  def aggregate[
      T,
      H1 <: TaggedAgg[_, _, _],
      H2 <: TaggedAgg[_, _, _],
      H3 <: TaggedAgg[_, _, _],
      R <: AggRes,
      Comb
  ](
      f1: A => H1,
      f2: A => H2,
      f3: A => H3
  )(implicit aggF: AggFunc[
      H1 %::
        H2 %::
        H3 %::
        DNil,
      R,
      Comb
    ]): Aggregate[
    F,
    Er,
    A,
    E,
    G,
    H1 %::
      H2 %::
      H3 %::
      DNil,
    R,
    Comb
  ] =
    new Aggregate[
      F,
      Er,
      A,
      E,
      G,
      H1 %::
        H2 %::
        H3 %::
        DNil,
      R,
      Comb
    ](pipeline,
      self.getG,
      a =>
        f1(a) %::
          f2(a) %::
          f3(a) %::
        DNil,
      self.filterOpt)

  def aggregate[
      T,
      H1 <: TaggedAgg[_, _, _],
      H2 <: TaggedAgg[_, _, _],
      H3 <: TaggedAgg[_, _, _],
      H4 <: TaggedAgg[_, _, _],
      R <: AggRes,
      Comb
  ](
      f1: A => H1,
      f2: A => H2,
      f3: A => H3,
      f4: A => H4
  )(implicit aggF: AggFunc[
      H1 %::
        H2 %::
        H3 %::
        H4 %::
        DNil,
      R,
      Comb
    ]): Aggregate[
    F,
    Er,
    A,
    E,
    G,
    H1 %::
      H2 %::
      H3 %::
      H4 %::
      DNil,
    R,
    Comb
  ] =
    new Aggregate[
      F,
      Er,
      A,
      E,
      G,
      H1 %::
        H2 %::
        H3 %::
        H4 %::
        DNil,
      R,
      Comb
    ](pipeline,
      self.getG,
      a =>
        f1(a) %::
          f2(a) %::
          f3(a) %::
          f4(a) %::
        DNil,
      self.filterOpt)

  def aggregate[
      T,
      H1 <: TaggedAgg[_, _, _],
      H2 <: TaggedAgg[_, _, _],
      H3 <: TaggedAgg[_, _, _],
      H4 <: TaggedAgg[_, _, _],
      H5 <: TaggedAgg[_, _, _],
      R <: AggRes,
      Comb
  ](
      f1: A => H1,
      f2: A => H2,
      f3: A => H3,
      f4: A => H4,
      f5: A => H5
  )(implicit aggF: AggFunc[
      H1 %::
        H2 %::
        H3 %::
        H4 %::
        H5 %::
        DNil,
      R,
      Comb
    ]): Aggregate[
    F,
    Er,
    A,
    E,
    G,
    H1 %::
      H2 %::
      H3 %::
      H4 %::
      H5 %::
      DNil,
    R,
    Comb
  ] =
    new Aggregate[
      F,
      Er,
      A,
      E,
      G,
      H1 %::
        H2 %::
        H3 %::
        H4 %::
        H5 %::
        DNil,
      R,
      Comb
    ](pipeline,
      self.getG,
      a =>
        f1(a) %::
          f2(a) %::
          f3(a) %::
          f4(a) %::
          f5(a) %::
        DNil,
      self.filterOpt)

  def aggregate[
      T,
      H1 <: TaggedAgg[_, _, _],
      H2 <: TaggedAgg[_, _, _],
      H3 <: TaggedAgg[_, _, _],
      H4 <: TaggedAgg[_, _, _],
      H5 <: TaggedAgg[_, _, _],
      H6 <: TaggedAgg[_, _, _],
      R <: AggRes,
      Comb
  ](
      f1: A => H1,
      f2: A => H2,
      f3: A => H3,
      f4: A => H4,
      f5: A => H5,
      f6: A => H6
  )(implicit aggF: AggFunc[
      H1 %::
        H2 %::
        H3 %::
        H4 %::
        H5 %::
        H6 %::
        DNil,
      R,
      Comb
    ]): Aggregate[
    F,
    Er,
    A,
    E,
    G,
    H1 %::
      H2 %::
      H3 %::
      H4 %::
      H5 %::
      H6 %::
      DNil,
    R,
    Comb
  ] =
    new Aggregate[
      F,
      Er,
      A,
      E,
      G,
      H1 %::
        H2 %::
        H3 %::
        H4 %::
        H5 %::
        H6 %::
        DNil,
      R,
      Comb
    ](pipeline,
      self.getG,
      a =>
        f1(a) %::
          f2(a) %::
          f3(a) %::
          f4(a) %::
          f5(a) %::
          f6(a) %::
        DNil,
      self.filterOpt)

  def aggregate[
      T,
      H1 <: TaggedAgg[_, _, _],
      H2 <: TaggedAgg[_, _, _],
      H3 <: TaggedAgg[_, _, _],
      H4 <: TaggedAgg[_, _, _],
      H5 <: TaggedAgg[_, _, _],
      H6 <: TaggedAgg[_, _, _],
      H7 <: TaggedAgg[_, _, _],
      R <: AggRes,
      Comb
  ](
      f1: A => H1,
      f2: A => H2,
      f3: A => H3,
      f4: A => H4,
      f5: A => H5,
      f6: A => H6,
      f7: A => H7
  )(implicit aggF: AggFunc[
      H1 %::
        H2 %::
        H3 %::
        H4 %::
        H5 %::
        H6 %::
        H7 %::
        DNil,
      R,
      Comb
    ]): Aggregate[
    F,
    Er,
    A,
    E,
    G,
    H1 %::
      H2 %::
      H3 %::
      H4 %::
      H5 %::
      H6 %::
      H7 %::
      DNil,
    R,
    Comb
  ] =
    new Aggregate[
      F,
      Er,
      A,
      E,
      G,
      H1 %::
        H2 %::
        H3 %::
        H4 %::
        H5 %::
        H6 %::
        H7 %::
        DNil,
      R,
      Comb
    ](pipeline,
      self.getG,
      a =>
        f1(a) %::
          f2(a) %::
          f3(a) %::
          f4(a) %::
          f5(a) %::
          f6(a) %::
          f7(a) %::
        DNil,
      self.filterOpt)

  def aggregate[
      T,
      H1 <: TaggedAgg[_, _, _],
      H2 <: TaggedAgg[_, _, _],
      H3 <: TaggedAgg[_, _, _],
      H4 <: TaggedAgg[_, _, _],
      H5 <: TaggedAgg[_, _, _],
      H6 <: TaggedAgg[_, _, _],
      H7 <: TaggedAgg[_, _, _],
      H8 <: TaggedAgg[_, _, _],
      R <: AggRes,
      Comb
  ](
      f1: A => H1,
      f2: A => H2,
      f3: A => H3,
      f4: A => H4,
      f5: A => H5,
      f6: A => H6,
      f7: A => H7,
      f8: A => H8
  )(implicit aggF: AggFunc[
      H1 %::
        H2 %::
        H3 %::
        H4 %::
        H5 %::
        H6 %::
        H7 %::
        H8 %::
        DNil,
      R,
      Comb
    ]): Aggregate[
    F,
    Er,
    A,
    E,
    G,
    H1 %::
      H2 %::
      H3 %::
      H4 %::
      H5 %::
      H6 %::
      H7 %::
      H8 %::
      DNil,
    R,
    Comb
  ] =
    new Aggregate[
      F,
      Er,
      A,
      E,
      G,
      H1 %::
        H2 %::
        H3 %::
        H4 %::
        H5 %::
        H6 %::
        H7 %::
        H8 %::
        DNil,
      R,
      Comb
    ](
      pipeline,
      self.getG,
      a =>
        f1(a) %::
          f2(a) %::
          f3(a) %::
          f4(a) %::
          f5(a) %::
          f6(a) %::
          f7(a) %::
          f8(a) %::
        DNil,
      self.filterOpt
    )

  def aggregate[
      T,
      H1 <: TaggedAgg[_, _, _],
      H2 <: TaggedAgg[_, _, _],
      H3 <: TaggedAgg[_, _, _],
      H4 <: TaggedAgg[_, _, _],
      H5 <: TaggedAgg[_, _, _],
      H6 <: TaggedAgg[_, _, _],
      H7 <: TaggedAgg[_, _, _],
      H8 <: TaggedAgg[_, _, _],
      H9 <: TaggedAgg[_, _, _],
      R <: AggRes,
      Comb
  ](
      f1: A => H1,
      f2: A => H2,
      f3: A => H3,
      f4: A => H4,
      f5: A => H5,
      f6: A => H6,
      f7: A => H7,
      f8: A => H8,
      f9: A => H9
  )(implicit aggF: AggFunc[
      H1 %::
        H2 %::
        H3 %::
        H4 %::
        H5 %::
        H6 %::
        H7 %::
        H8 %::
        H9 %::
        DNil,
      R,
      Comb
    ]): Aggregate[
    F,
    Er,
    A,
    E,
    G,
    H1 %::
      H2 %::
      H3 %::
      H4 %::
      H5 %::
      H6 %::
      H7 %::
      H8 %::
      H9 %::
      DNil,
    R,
    Comb
  ] =
    new Aggregate[
      F,
      Er,
      A,
      E,
      G,
      H1 %::
        H2 %::
        H3 %::
        H4 %::
        H5 %::
        H6 %::
        H7 %::
        H8 %::
        H9 %::
        DNil,
      R,
      Comb
    ](
      pipeline,
      self.getG,
      a =>
        f1(a) %::
          f2(a) %::
          f3(a) %::
          f4(a) %::
          f5(a) %::
          f6(a) %::
          f7(a) %::
          f8(a) %::
          f9(a) %::
        DNil,
      self.filterOpt
    )

  def aggregate[
      T,
      H1 <: TaggedAgg[_, _, _],
      H2 <: TaggedAgg[_, _, _],
      H3 <: TaggedAgg[_, _, _],
      H4 <: TaggedAgg[_, _, _],
      H5 <: TaggedAgg[_, _, _],
      H6 <: TaggedAgg[_, _, _],
      H7 <: TaggedAgg[_, _, _],
      H8 <: TaggedAgg[_, _, _],
      H9 <: TaggedAgg[_, _, _],
      H10 <: TaggedAgg[_, _, _],
      R <: AggRes,
      Comb
  ](
      f1: A => H1,
      f2: A => H2,
      f3: A => H3,
      f4: A => H4,
      f5: A => H5,
      f6: A => H6,
      f7: A => H7,
      f8: A => H8,
      f9: A => H9,
      f10: A => H10
  )(implicit aggF: AggFunc[
      H1 %::
        H2 %::
        H3 %::
        H4 %::
        H5 %::
        H6 %::
        H7 %::
        H8 %::
        H9 %::
        H10 %::
        DNil,
      R,
      Comb
    ]): Aggregate[
    F,
    Er,
    A,
    E,
    G,
    H1 %::
      H2 %::
      H3 %::
      H4 %::
      H5 %::
      H6 %::
      H7 %::
      H8 %::
      H9 %::
      H10 %::
      DNil,
    R,
    Comb
  ] =
    new Aggregate[
      F,
      Er,
      A,
      E,
      G,
      H1 %::
        H2 %::
        H3 %::
        H4 %::
        H5 %::
        H6 %::
        H7 %::
        H8 %::
        H9 %::
        H10 %::
        DNil,
      R,
      Comb
    ](
      pipeline,
      self.getG,
      a =>
        f1(a) %::
          f2(a) %::
          f3(a) %::
          f4(a) %::
          f5(a) %::
          f6(a) %::
          f7(a) %::
          f8(a) %::
          f9(a) %::
          f10(a) %::
        DNil,
      self.filterOpt
    )

  def aggregate[
      T,
      H1 <: TaggedAgg[_, _, _],
      H2 <: TaggedAgg[_, _, _],
      H3 <: TaggedAgg[_, _, _],
      H4 <: TaggedAgg[_, _, _],
      H5 <: TaggedAgg[_, _, _],
      H6 <: TaggedAgg[_, _, _],
      H7 <: TaggedAgg[_, _, _],
      H8 <: TaggedAgg[_, _, _],
      H9 <: TaggedAgg[_, _, _],
      H10 <: TaggedAgg[_, _, _],
      H11 <: TaggedAgg[_, _, _],
      R <: AggRes,
      Comb
  ](
      f1: A => H1,
      f2: A => H2,
      f3: A => H3,
      f4: A => H4,
      f5: A => H5,
      f6: A => H6,
      f7: A => H7,
      f8: A => H8,
      f9: A => H9,
      f10: A => H10,
      f11: A => H11
  )(implicit aggF: AggFunc[
      H1 %::
        H2 %::
        H3 %::
        H4 %::
        H5 %::
        H6 %::
        H7 %::
        H8 %::
        H9 %::
        H10 %::
        H11 %::
        DNil,
      R,
      Comb
    ]): Aggregate[
    F,
    Er,
    A,
    E,
    G,
    H1 %::
      H2 %::
      H3 %::
      H4 %::
      H5 %::
      H6 %::
      H7 %::
      H8 %::
      H9 %::
      H10 %::
      H11 %::
      DNil,
    R,
    Comb
  ] =
    new Aggregate[
      F,
      Er,
      A,
      E,
      G,
      H1 %::
        H2 %::
        H3 %::
        H4 %::
        H5 %::
        H6 %::
        H7 %::
        H8 %::
        H9 %::
        H10 %::
        H11 %::
        DNil,
      R,
      Comb
    ](
      pipeline,
      self.getG,
      a =>
        f1(a) %::
          f2(a) %::
          f3(a) %::
          f4(a) %::
          f5(a) %::
          f6(a) %::
          f7(a) %::
          f8(a) %::
          f9(a) %::
          f10(a) %::
          f11(a) %::
        DNil,
      self.filterOpt
    )

  def aggregate[
      T,
      H1 <: TaggedAgg[_, _, _],
      H2 <: TaggedAgg[_, _, _],
      H3 <: TaggedAgg[_, _, _],
      H4 <: TaggedAgg[_, _, _],
      H5 <: TaggedAgg[_, _, _],
      H6 <: TaggedAgg[_, _, _],
      H7 <: TaggedAgg[_, _, _],
      H8 <: TaggedAgg[_, _, _],
      H9 <: TaggedAgg[_, _, _],
      H10 <: TaggedAgg[_, _, _],
      H11 <: TaggedAgg[_, _, _],
      H12 <: TaggedAgg[_, _, _],
      R <: AggRes,
      Comb
  ](
      f1: A => H1,
      f2: A => H2,
      f3: A => H3,
      f4: A => H4,
      f5: A => H5,
      f6: A => H6,
      f7: A => H7,
      f8: A => H8,
      f9: A => H9,
      f10: A => H10,
      f11: A => H11,
      f12: A => H12
  )(implicit aggF: AggFunc[
      H1 %::
        H2 %::
        H3 %::
        H4 %::
        H5 %::
        H6 %::
        H7 %::
        H8 %::
        H9 %::
        H10 %::
        H11 %::
        H12 %::
        DNil,
      R,
      Comb
    ]): Aggregate[
    F,
    Er,
    A,
    E,
    G,
    H1 %::
      H2 %::
      H3 %::
      H4 %::
      H5 %::
      H6 %::
      H7 %::
      H8 %::
      H9 %::
      H10 %::
      H11 %::
      H12 %::
      DNil,
    R,
    Comb
  ] =
    new Aggregate[
      F,
      Er,
      A,
      E,
      G,
      H1 %::
        H2 %::
        H3 %::
        H4 %::
        H5 %::
        H6 %::
        H7 %::
        H8 %::
        H9 %::
        H10 %::
        H11 %::
        H12 %::
        DNil,
      R,
      Comb
    ](
      pipeline,
      self.getG,
      a =>
        f1(a) %::
          f2(a) %::
          f3(a) %::
          f4(a) %::
          f5(a) %::
          f6(a) %::
          f7(a) %::
          f8(a) %::
          f9(a) %::
          f10(a) %::
          f11(a) %::
          f12(a) %::
        DNil,
      self.filterOpt
    )

  def aggregate[
      T,
      H1 <: TaggedAgg[_, _, _],
      H2 <: TaggedAgg[_, _, _],
      H3 <: TaggedAgg[_, _, _],
      H4 <: TaggedAgg[_, _, _],
      H5 <: TaggedAgg[_, _, _],
      H6 <: TaggedAgg[_, _, _],
      H7 <: TaggedAgg[_, _, _],
      H8 <: TaggedAgg[_, _, _],
      H9 <: TaggedAgg[_, _, _],
      H10 <: TaggedAgg[_, _, _],
      H11 <: TaggedAgg[_, _, _],
      H12 <: TaggedAgg[_, _, _],
      H13 <: TaggedAgg[_, _, _],
      R <: AggRes,
      Comb
  ](
      f1: A => H1,
      f2: A => H2,
      f3: A => H3,
      f4: A => H4,
      f5: A => H5,
      f6: A => H6,
      f7: A => H7,
      f8: A => H8,
      f9: A => H9,
      f10: A => H10,
      f11: A => H11,
      f12: A => H12,
      f13: A => H13
  )(implicit aggF: AggFunc[
      H1 %::
        H2 %::
        H3 %::
        H4 %::
        H5 %::
        H6 %::
        H7 %::
        H8 %::
        H9 %::
        H10 %::
        H11 %::
        H12 %::
        H13 %::
        DNil,
      R,
      Comb
    ]): Aggregate[
    F,
    Er,
    A,
    E,
    G,
    H1 %::
      H2 %::
      H3 %::
      H4 %::
      H5 %::
      H6 %::
      H7 %::
      H8 %::
      H9 %::
      H10 %::
      H11 %::
      H12 %::
      H13 %::
      DNil,
    R,
    Comb
  ] =
    new Aggregate[
      F,
      Er,
      A,
      E,
      G,
      H1 %::
        H2 %::
        H3 %::
        H4 %::
        H5 %::
        H6 %::
        H7 %::
        H8 %::
        H9 %::
        H10 %::
        H11 %::
        H12 %::
        H13 %::
        DNil,
      R,
      Comb
    ](
      pipeline,
      self.getG,
      a =>
        f1(a) %::
          f2(a) %::
          f3(a) %::
          f4(a) %::
          f5(a) %::
          f6(a) %::
          f7(a) %::
          f8(a) %::
          f9(a) %::
          f10(a) %::
          f11(a) %::
          f12(a) %::
          f13(a) %::
        DNil,
      self.filterOpt
    )

  def aggregate[
      T,
      H1 <: TaggedAgg[_, _, _],
      H2 <: TaggedAgg[_, _, _],
      H3 <: TaggedAgg[_, _, _],
      H4 <: TaggedAgg[_, _, _],
      H5 <: TaggedAgg[_, _, _],
      H6 <: TaggedAgg[_, _, _],
      H7 <: TaggedAgg[_, _, _],
      H8 <: TaggedAgg[_, _, _],
      H9 <: TaggedAgg[_, _, _],
      H10 <: TaggedAgg[_, _, _],
      H11 <: TaggedAgg[_, _, _],
      H12 <: TaggedAgg[_, _, _],
      H13 <: TaggedAgg[_, _, _],
      H14 <: TaggedAgg[_, _, _],
      R <: AggRes,
      Comb
  ](
      f1: A => H1,
      f2: A => H2,
      f3: A => H3,
      f4: A => H4,
      f5: A => H5,
      f6: A => H6,
      f7: A => H7,
      f8: A => H8,
      f9: A => H9,
      f10: A => H10,
      f11: A => H11,
      f12: A => H12,
      f13: A => H13,
      f14: A => H14
  )(implicit aggF: AggFunc[
      H1 %::
        H2 %::
        H3 %::
        H4 %::
        H5 %::
        H6 %::
        H7 %::
        H8 %::
        H9 %::
        H10 %::
        H11 %::
        H12 %::
        H13 %::
        H14 %::
        DNil,
      R,
      Comb
    ]): Aggregate[
    F,
    Er,
    A,
    E,
    G,
    H1 %::
      H2 %::
      H3 %::
      H4 %::
      H5 %::
      H6 %::
      H7 %::
      H8 %::
      H9 %::
      H10 %::
      H11 %::
      H12 %::
      H13 %::
      H14 %::
      DNil,
    R,
    Comb
  ] =
    new Aggregate[
      F,
      Er,
      A,
      E,
      G,
      H1 %::
        H2 %::
        H3 %::
        H4 %::
        H5 %::
        H6 %::
        H7 %::
        H8 %::
        H9 %::
        H10 %::
        H11 %::
        H12 %::
        H13 %::
        H14 %::
        DNil,
      R,
      Comb
    ](
      pipeline,
      self.getG,
      a =>
        f1(a) %::
          f2(a) %::
          f3(a) %::
          f4(a) %::
          f5(a) %::
          f6(a) %::
          f7(a) %::
          f8(a) %::
          f9(a) %::
          f10(a) %::
          f11(a) %::
          f12(a) %::
          f13(a) %::
          f14(a) %::
        DNil,
      self.filterOpt
    )

  def aggregate[
      T,
      H1 <: TaggedAgg[_, _, _],
      H2 <: TaggedAgg[_, _, _],
      H3 <: TaggedAgg[_, _, _],
      H4 <: TaggedAgg[_, _, _],
      H5 <: TaggedAgg[_, _, _],
      H6 <: TaggedAgg[_, _, _],
      H7 <: TaggedAgg[_, _, _],
      H8 <: TaggedAgg[_, _, _],
      H9 <: TaggedAgg[_, _, _],
      H10 <: TaggedAgg[_, _, _],
      H11 <: TaggedAgg[_, _, _],
      H12 <: TaggedAgg[_, _, _],
      H13 <: TaggedAgg[_, _, _],
      H14 <: TaggedAgg[_, _, _],
      H15 <: TaggedAgg[_, _, _],
      R <: AggRes,
      Comb
  ](
      f1: A => H1,
      f2: A => H2,
      f3: A => H3,
      f4: A => H4,
      f5: A => H5,
      f6: A => H6,
      f7: A => H7,
      f8: A => H8,
      f9: A => H9,
      f10: A => H10,
      f11: A => H11,
      f12: A => H12,
      f13: A => H13,
      f14: A => H14,
      f15: A => H15
  )(implicit aggF: AggFunc[
      H1 %::
        H2 %::
        H3 %::
        H4 %::
        H5 %::
        H6 %::
        H7 %::
        H8 %::
        H9 %::
        H10 %::
        H11 %::
        H12 %::
        H13 %::
        H14 %::
        H15 %::
        DNil,
      R,
      Comb
    ]): Aggregate[
    F,
    Er,
    A,
    E,
    G,
    H1 %::
      H2 %::
      H3 %::
      H4 %::
      H5 %::
      H6 %::
      H7 %::
      H8 %::
      H9 %::
      H10 %::
      H11 %::
      H12 %::
      H13 %::
      H14 %::
      H15 %::
      DNil,
    R,
    Comb
  ] =
    new Aggregate[
      F,
      Er,
      A,
      E,
      G,
      H1 %::
        H2 %::
        H3 %::
        H4 %::
        H5 %::
        H6 %::
        H7 %::
        H8 %::
        H9 %::
        H10 %::
        H11 %::
        H12 %::
        H13 %::
        H14 %::
        H15 %::
        DNil,
      R,
      Comb
    ](
      pipeline,
      self.getG,
      a =>
        f1(a) %::
          f2(a) %::
          f3(a) %::
          f4(a) %::
          f5(a) %::
          f6(a) %::
          f7(a) %::
          f8(a) %::
          f9(a) %::
          f10(a) %::
          f11(a) %::
          f12(a) %::
          f13(a) %::
          f14(a) %::
          f15(a) %::
        DNil,
      self.filterOpt
    )

  def aggregate[
      T,
      H1 <: TaggedAgg[_, _, _],
      H2 <: TaggedAgg[_, _, _],
      H3 <: TaggedAgg[_, _, _],
      H4 <: TaggedAgg[_, _, _],
      H5 <: TaggedAgg[_, _, _],
      H6 <: TaggedAgg[_, _, _],
      H7 <: TaggedAgg[_, _, _],
      H8 <: TaggedAgg[_, _, _],
      H9 <: TaggedAgg[_, _, _],
      H10 <: TaggedAgg[_, _, _],
      H11 <: TaggedAgg[_, _, _],
      H12 <: TaggedAgg[_, _, _],
      H13 <: TaggedAgg[_, _, _],
      H14 <: TaggedAgg[_, _, _],
      H15 <: TaggedAgg[_, _, _],
      H16 <: TaggedAgg[_, _, _],
      R <: AggRes,
      Comb
  ](
      f1: A => H1,
      f2: A => H2,
      f3: A => H3,
      f4: A => H4,
      f5: A => H5,
      f6: A => H6,
      f7: A => H7,
      f8: A => H8,
      f9: A => H9,
      f10: A => H10,
      f11: A => H11,
      f12: A => H12,
      f13: A => H13,
      f14: A => H14,
      f15: A => H15,
      f16: A => H16
  )(implicit aggF: AggFunc[
      H1 %::
        H2 %::
        H3 %::
        H4 %::
        H5 %::
        H6 %::
        H7 %::
        H8 %::
        H9 %::
        H10 %::
        H11 %::
        H12 %::
        H13 %::
        H14 %::
        H15 %::
        H16 %::
        DNil,
      R,
      Comb
    ]): Aggregate[
    F,
    Er,
    A,
    E,
    G,
    H1 %::
      H2 %::
      H3 %::
      H4 %::
      H5 %::
      H6 %::
      H7 %::
      H8 %::
      H9 %::
      H10 %::
      H11 %::
      H12 %::
      H13 %::
      H14 %::
      H15 %::
      H16 %::
      DNil,
    R,
    Comb
  ] =
    new Aggregate[
      F,
      Er,
      A,
      E,
      G,
      H1 %::
        H2 %::
        H3 %::
        H4 %::
        H5 %::
        H6 %::
        H7 %::
        H8 %::
        H9 %::
        H10 %::
        H11 %::
        H12 %::
        H13 %::
        H14 %::
        H15 %::
        H16 %::
        DNil,
      R,
      Comb
    ](
      pipeline,
      self.getG,
      a =>
        f1(a) %::
          f2(a) %::
          f3(a) %::
          f4(a) %::
          f5(a) %::
          f6(a) %::
          f7(a) %::
          f8(a) %::
          f9(a) %::
          f10(a) %::
          f11(a) %::
          f12(a) %::
          f13(a) %::
          f14(a) %::
          f15(a) %::
          f16(a) %::
        DNil,
      self.filterOpt
    )

  def aggregate[
      T,
      H1 <: TaggedAgg[_, _, _],
      H2 <: TaggedAgg[_, _, _],
      H3 <: TaggedAgg[_, _, _],
      H4 <: TaggedAgg[_, _, _],
      H5 <: TaggedAgg[_, _, _],
      H6 <: TaggedAgg[_, _, _],
      H7 <: TaggedAgg[_, _, _],
      H8 <: TaggedAgg[_, _, _],
      H9 <: TaggedAgg[_, _, _],
      H10 <: TaggedAgg[_, _, _],
      H11 <: TaggedAgg[_, _, _],
      H12 <: TaggedAgg[_, _, _],
      H13 <: TaggedAgg[_, _, _],
      H14 <: TaggedAgg[_, _, _],
      H15 <: TaggedAgg[_, _, _],
      H16 <: TaggedAgg[_, _, _],
      H17 <: TaggedAgg[_, _, _],
      R <: AggRes,
      Comb
  ](
      f1: A => H1,
      f2: A => H2,
      f3: A => H3,
      f4: A => H4,
      f5: A => H5,
      f6: A => H6,
      f7: A => H7,
      f8: A => H8,
      f9: A => H9,
      f10: A => H10,
      f11: A => H11,
      f12: A => H12,
      f13: A => H13,
      f14: A => H14,
      f15: A => H15,
      f16: A => H16,
      f17: A => H17
  )(implicit aggF: AggFunc[
      H1 %::
        H2 %::
        H3 %::
        H4 %::
        H5 %::
        H6 %::
        H7 %::
        H8 %::
        H9 %::
        H10 %::
        H11 %::
        H12 %::
        H13 %::
        H14 %::
        H15 %::
        H16 %::
        H17 %::
        DNil,
      R,
      Comb
    ]): Aggregate[
    F,
    Er,
    A,
    E,
    G,
    H1 %::
      H2 %::
      H3 %::
      H4 %::
      H5 %::
      H6 %::
      H7 %::
      H8 %::
      H9 %::
      H10 %::
      H11 %::
      H12 %::
      H13 %::
      H14 %::
      H15 %::
      H16 %::
      H17 %::
      DNil,
    R,
    Comb
  ] =
    new Aggregate[
      F,
      Er,
      A,
      E,
      G,
      H1 %::
        H2 %::
        H3 %::
        H4 %::
        H5 %::
        H6 %::
        H7 %::
        H8 %::
        H9 %::
        H10 %::
        H11 %::
        H12 %::
        H13 %::
        H14 %::
        H15 %::
        H16 %::
        H17 %::
        DNil,
      R,
      Comb
    ](
      pipeline,
      self.getG,
      a =>
        f1(a) %::
          f2(a) %::
          f3(a) %::
          f4(a) %::
          f5(a) %::
          f6(a) %::
          f7(a) %::
          f8(a) %::
          f9(a) %::
          f10(a) %::
          f11(a) %::
          f12(a) %::
          f13(a) %::
          f14(a) %::
          f15(a) %::
          f16(a) %::
          f17(a) %::
        DNil,
      self.filterOpt
    )

  def aggregate[
      T,
      H1 <: TaggedAgg[_, _, _],
      H2 <: TaggedAgg[_, _, _],
      H3 <: TaggedAgg[_, _, _],
      H4 <: TaggedAgg[_, _, _],
      H5 <: TaggedAgg[_, _, _],
      H6 <: TaggedAgg[_, _, _],
      H7 <: TaggedAgg[_, _, _],
      H8 <: TaggedAgg[_, _, _],
      H9 <: TaggedAgg[_, _, _],
      H10 <: TaggedAgg[_, _, _],
      H11 <: TaggedAgg[_, _, _],
      H12 <: TaggedAgg[_, _, _],
      H13 <: TaggedAgg[_, _, _],
      H14 <: TaggedAgg[_, _, _],
      H15 <: TaggedAgg[_, _, _],
      H16 <: TaggedAgg[_, _, _],
      H17 <: TaggedAgg[_, _, _],
      H18 <: TaggedAgg[_, _, _],
      R <: AggRes,
      Comb
  ](
      f1: A => H1,
      f2: A => H2,
      f3: A => H3,
      f4: A => H4,
      f5: A => H5,
      f6: A => H6,
      f7: A => H7,
      f8: A => H8,
      f9: A => H9,
      f10: A => H10,
      f11: A => H11,
      f12: A => H12,
      f13: A => H13,
      f14: A => H14,
      f15: A => H15,
      f16: A => H16,
      f17: A => H17,
      f18: A => H18
  )(implicit aggF: AggFunc[
      H1 %::
        H2 %::
        H3 %::
        H4 %::
        H5 %::
        H6 %::
        H7 %::
        H8 %::
        H9 %::
        H10 %::
        H11 %::
        H12 %::
        H13 %::
        H14 %::
        H15 %::
        H16 %::
        H17 %::
        H18 %::
        DNil,
      R,
      Comb
    ]): Aggregate[
    F,
    Er,
    A,
    E,
    G,
    H1 %::
      H2 %::
      H3 %::
      H4 %::
      H5 %::
      H6 %::
      H7 %::
      H8 %::
      H9 %::
      H10 %::
      H11 %::
      H12 %::
      H13 %::
      H14 %::
      H15 %::
      H16 %::
      H17 %::
      H18 %::
      DNil,
    R,
    Comb
  ] =
    new Aggregate[
      F,
      Er,
      A,
      E,
      G,
      H1 %::
        H2 %::
        H3 %::
        H4 %::
        H5 %::
        H6 %::
        H7 %::
        H8 %::
        H9 %::
        H10 %::
        H11 %::
        H12 %::
        H13 %::
        H14 %::
        H15 %::
        H16 %::
        H17 %::
        H18 %::
        DNil,
      R,
      Comb
    ](
      pipeline,
      self.getG,
      a =>
        f1(a) %::
          f2(a) %::
          f3(a) %::
          f4(a) %::
          f5(a) %::
          f6(a) %::
          f7(a) %::
          f8(a) %::
          f9(a) %::
          f10(a) %::
          f11(a) %::
          f12(a) %::
          f13(a) %::
          f14(a) %::
          f15(a) %::
          f16(a) %::
          f17(a) %::
          f18(a) %::
        DNil,
      self.filterOpt
    )

  def aggregate[
      T,
      H1 <: TaggedAgg[_, _, _],
      H2 <: TaggedAgg[_, _, _],
      H3 <: TaggedAgg[_, _, _],
      H4 <: TaggedAgg[_, _, _],
      H5 <: TaggedAgg[_, _, _],
      H6 <: TaggedAgg[_, _, _],
      H7 <: TaggedAgg[_, _, _],
      H8 <: TaggedAgg[_, _, _],
      H9 <: TaggedAgg[_, _, _],
      H10 <: TaggedAgg[_, _, _],
      H11 <: TaggedAgg[_, _, _],
      H12 <: TaggedAgg[_, _, _],
      H13 <: TaggedAgg[_, _, _],
      H14 <: TaggedAgg[_, _, _],
      H15 <: TaggedAgg[_, _, _],
      H16 <: TaggedAgg[_, _, _],
      H17 <: TaggedAgg[_, _, _],
      H18 <: TaggedAgg[_, _, _],
      H19 <: TaggedAgg[_, _, _],
      R <: AggRes,
      Comb
  ](
      f1: A => H1,
      f2: A => H2,
      f3: A => H3,
      f4: A => H4,
      f5: A => H5,
      f6: A => H6,
      f7: A => H7,
      f8: A => H8,
      f9: A => H9,
      f10: A => H10,
      f11: A => H11,
      f12: A => H12,
      f13: A => H13,
      f14: A => H14,
      f15: A => H15,
      f16: A => H16,
      f17: A => H17,
      f18: A => H18,
      f19: A => H19
  )(implicit aggF: AggFunc[
      H1 %::
        H2 %::
        H3 %::
        H4 %::
        H5 %::
        H6 %::
        H7 %::
        H8 %::
        H9 %::
        H10 %::
        H11 %::
        H12 %::
        H13 %::
        H14 %::
        H15 %::
        H16 %::
        H17 %::
        H18 %::
        H19 %::
        DNil,
      R,
      Comb
    ]): Aggregate[
    F,
    Er,
    A,
    E,
    G,
    H1 %::
      H2 %::
      H3 %::
      H4 %::
      H5 %::
      H6 %::
      H7 %::
      H8 %::
      H9 %::
      H10 %::
      H11 %::
      H12 %::
      H13 %::
      H14 %::
      H15 %::
      H16 %::
      H17 %::
      H18 %::
      H19 %::
      DNil,
    R,
    Comb
  ] =
    new Aggregate[
      F,
      Er,
      A,
      E,
      G,
      H1 %::
        H2 %::
        H3 %::
        H4 %::
        H5 %::
        H6 %::
        H7 %::
        H8 %::
        H9 %::
        H10 %::
        H11 %::
        H12 %::
        H13 %::
        H14 %::
        H15 %::
        H16 %::
        H17 %::
        H18 %::
        H19 %::
        DNil,
      R,
      Comb
    ](
      pipeline,
      self.getG,
      a =>
        f1(a) %::
          f2(a) %::
          f3(a) %::
          f4(a) %::
          f5(a) %::
          f6(a) %::
          f7(a) %::
          f8(a) %::
          f9(a) %::
          f10(a) %::
          f11(a) %::
          f12(a) %::
          f13(a) %::
          f14(a) %::
          f15(a) %::
          f16(a) %::
          f17(a) %::
          f18(a) %::
          f19(a) %::
        DNil,
      self.filterOpt
    )

  def aggregate[
      T,
      H1 <: TaggedAgg[_, _, _],
      H2 <: TaggedAgg[_, _, _],
      H3 <: TaggedAgg[_, _, _],
      H4 <: TaggedAgg[_, _, _],
      H5 <: TaggedAgg[_, _, _],
      H6 <: TaggedAgg[_, _, _],
      H7 <: TaggedAgg[_, _, _],
      H8 <: TaggedAgg[_, _, _],
      H9 <: TaggedAgg[_, _, _],
      H10 <: TaggedAgg[_, _, _],
      H11 <: TaggedAgg[_, _, _],
      H12 <: TaggedAgg[_, _, _],
      H13 <: TaggedAgg[_, _, _],
      H14 <: TaggedAgg[_, _, _],
      H15 <: TaggedAgg[_, _, _],
      H16 <: TaggedAgg[_, _, _],
      H17 <: TaggedAgg[_, _, _],
      H18 <: TaggedAgg[_, _, _],
      H19 <: TaggedAgg[_, _, _],
      H20 <: TaggedAgg[_, _, _],
      R <: AggRes,
      Comb
  ](
      f1: A => H1,
      f2: A => H2,
      f3: A => H3,
      f4: A => H4,
      f5: A => H5,
      f6: A => H6,
      f7: A => H7,
      f8: A => H8,
      f9: A => H9,
      f10: A => H10,
      f11: A => H11,
      f12: A => H12,
      f13: A => H13,
      f14: A => H14,
      f15: A => H15,
      f16: A => H16,
      f17: A => H17,
      f18: A => H18,
      f19: A => H19,
      f20: A => H20
  )(implicit aggF: AggFunc[
      H1 %::
        H2 %::
        H3 %::
        H4 %::
        H5 %::
        H6 %::
        H7 %::
        H8 %::
        H9 %::
        H10 %::
        H11 %::
        H12 %::
        H13 %::
        H14 %::
        H15 %::
        H16 %::
        H17 %::
        H18 %::
        H19 %::
        H20 %::
        DNil,
      R,
      Comb
    ]): Aggregate[
    F,
    Er,
    A,
    E,
    G,
    H1 %::
      H2 %::
      H3 %::
      H4 %::
      H5 %::
      H6 %::
      H7 %::
      H8 %::
      H9 %::
      H10 %::
      H11 %::
      H12 %::
      H13 %::
      H14 %::
      H15 %::
      H16 %::
      H17 %::
      H18 %::
      H19 %::
      H20 %::
      DNil,
    R,
    Comb
  ] =
    new Aggregate[
      F,
      Er,
      A,
      E,
      G,
      H1 %::
        H2 %::
        H3 %::
        H4 %::
        H5 %::
        H6 %::
        H7 %::
        H8 %::
        H9 %::
        H10 %::
        H11 %::
        H12 %::
        H13 %::
        H14 %::
        H15 %::
        H16 %::
        H17 %::
        H18 %::
        H19 %::
        H20 %::
        DNil,
      R,
      Comb
    ](
      pipeline,
      self.getG,
      a =>
        f1(a) %::
          f2(a) %::
          f3(a) %::
          f4(a) %::
          f5(a) %::
          f6(a) %::
          f7(a) %::
          f8(a) %::
          f9(a) %::
          f10(a) %::
          f11(a) %::
          f12(a) %::
          f13(a) %::
          f14(a) %::
          f15(a) %::
          f16(a) %::
          f17(a) %::
          f18(a) %::
          f19(a) %::
          f20(a) %::
        DNil,
      self.filterOpt
    )

  def aggregate[
      T,
      H1 <: TaggedAgg[_, _, _],
      H2 <: TaggedAgg[_, _, _],
      H3 <: TaggedAgg[_, _, _],
      H4 <: TaggedAgg[_, _, _],
      H5 <: TaggedAgg[_, _, _],
      H6 <: TaggedAgg[_, _, _],
      H7 <: TaggedAgg[_, _, _],
      H8 <: TaggedAgg[_, _, _],
      H9 <: TaggedAgg[_, _, _],
      H10 <: TaggedAgg[_, _, _],
      H11 <: TaggedAgg[_, _, _],
      H12 <: TaggedAgg[_, _, _],
      H13 <: TaggedAgg[_, _, _],
      H14 <: TaggedAgg[_, _, _],
      H15 <: TaggedAgg[_, _, _],
      H16 <: TaggedAgg[_, _, _],
      H17 <: TaggedAgg[_, _, _],
      H18 <: TaggedAgg[_, _, _],
      H19 <: TaggedAgg[_, _, _],
      H20 <: TaggedAgg[_, _, _],
      H21 <: TaggedAgg[_, _, _],
      R <: AggRes,
      Comb
  ](
      f1: A => H1,
      f2: A => H2,
      f3: A => H3,
      f4: A => H4,
      f5: A => H5,
      f6: A => H6,
      f7: A => H7,
      f8: A => H8,
      f9: A => H9,
      f10: A => H10,
      f11: A => H11,
      f12: A => H12,
      f13: A => H13,
      f14: A => H14,
      f15: A => H15,
      f16: A => H16,
      f17: A => H17,
      f18: A => H18,
      f19: A => H19,
      f20: A => H20,
      f21: A => H21
  )(implicit aggF: AggFunc[
      H1 %::
        H2 %::
        H3 %::
        H4 %::
        H5 %::
        H6 %::
        H7 %::
        H8 %::
        H9 %::
        H10 %::
        H11 %::
        H12 %::
        H13 %::
        H14 %::
        H15 %::
        H16 %::
        H17 %::
        H18 %::
        H19 %::
        H20 %::
        H21 %::
        DNil,
      R,
      Comb
    ]): Aggregate[
    F,
    Er,
    A,
    E,
    G,
    H1 %::
      H2 %::
      H3 %::
      H4 %::
      H5 %::
      H6 %::
      H7 %::
      H8 %::
      H9 %::
      H10 %::
      H11 %::
      H12 %::
      H13 %::
      H14 %::
      H15 %::
      H16 %::
      H17 %::
      H18 %::
      H19 %::
      H20 %::
      H21 %::
      DNil,
    R,
    Comb
  ] =
    new Aggregate[
      F,
      Er,
      A,
      E,
      G,
      H1 %::
        H2 %::
        H3 %::
        H4 %::
        H5 %::
        H6 %::
        H7 %::
        H8 %::
        H9 %::
        H10 %::
        H11 %::
        H12 %::
        H13 %::
        H14 %::
        H15 %::
        H16 %::
        H17 %::
        H18 %::
        H19 %::
        H20 %::
        H21 %::
        DNil,
      R,
      Comb
    ](
      pipeline,
      self.getG,
      a =>
        f1(a) %::
          f2(a) %::
          f3(a) %::
          f4(a) %::
          f5(a) %::
          f6(a) %::
          f7(a) %::
          f8(a) %::
          f9(a) %::
          f10(a) %::
          f11(a) %::
          f12(a) %::
          f13(a) %::
          f14(a) %::
          f15(a) %::
          f16(a) %::
          f17(a) %::
          f18(a) %::
          f19(a) %::
          f20(a) %::
          f21(a) %::
        DNil,
      self.filterOpt
    )

  def aggregate[
      T,
      H1 <: TaggedAgg[_, _, _],
      H2 <: TaggedAgg[_, _, _],
      H3 <: TaggedAgg[_, _, _],
      H4 <: TaggedAgg[_, _, _],
      H5 <: TaggedAgg[_, _, _],
      H6 <: TaggedAgg[_, _, _],
      H7 <: TaggedAgg[_, _, _],
      H8 <: TaggedAgg[_, _, _],
      H9 <: TaggedAgg[_, _, _],
      H10 <: TaggedAgg[_, _, _],
      H11 <: TaggedAgg[_, _, _],
      H12 <: TaggedAgg[_, _, _],
      H13 <: TaggedAgg[_, _, _],
      H14 <: TaggedAgg[_, _, _],
      H15 <: TaggedAgg[_, _, _],
      H16 <: TaggedAgg[_, _, _],
      H17 <: TaggedAgg[_, _, _],
      H18 <: TaggedAgg[_, _, _],
      H19 <: TaggedAgg[_, _, _],
      H20 <: TaggedAgg[_, _, _],
      H21 <: TaggedAgg[_, _, _],
      H22 <: TaggedAgg[_, _, _],
      R <: AggRes,
      Comb
  ](
      f1: A => H1,
      f2: A => H2,
      f3: A => H3,
      f4: A => H4,
      f5: A => H5,
      f6: A => H6,
      f7: A => H7,
      f8: A => H8,
      f9: A => H9,
      f10: A => H10,
      f11: A => H11,
      f12: A => H12,
      f13: A => H13,
      f14: A => H14,
      f15: A => H15,
      f16: A => H16,
      f17: A => H17,
      f18: A => H18,
      f19: A => H19,
      f20: A => H20,
      f21: A => H21,
      f22: A => H22
  )(implicit aggF: AggFunc[
      H1 %::
        H2 %::
        H3 %::
        H4 %::
        H5 %::
        H6 %::
        H7 %::
        H8 %::
        H9 %::
        H10 %::
        H11 %::
        H12 %::
        H13 %::
        H14 %::
        H15 %::
        H16 %::
        H17 %::
        H18 %::
        H19 %::
        H20 %::
        H21 %::
        H22 %::
        DNil,
      R,
      Comb
    ]): Aggregate[
    F,
    Er,
    A,
    E,
    G,
    H1 %::
      H2 %::
      H3 %::
      H4 %::
      H5 %::
      H6 %::
      H7 %::
      H8 %::
      H9 %::
      H10 %::
      H11 %::
      H12 %::
      H13 %::
      H14 %::
      H15 %::
      H16 %::
      H17 %::
      H18 %::
      H19 %::
      H20 %::
      H21 %::
      H22 %::
      DNil,
    R,
    Comb
  ] =
    new Aggregate[
      F,
      Er,
      A,
      E,
      G,
      H1 %::
        H2 %::
        H3 %::
        H4 %::
        H5 %::
        H6 %::
        H7 %::
        H8 %::
        H9 %::
        H10 %::
        H11 %::
        H12 %::
        H13 %::
        H14 %::
        H15 %::
        H16 %::
        H17 %::
        H18 %::
        H19 %::
        H20 %::
        H21 %::
        H22 %::
        DNil,
      R,
      Comb
    ](
      pipeline,
      self.getG,
      a =>
        f1(a) %::
          f2(a) %::
          f3(a) %::
          f4(a) %::
          f5(a) %::
          f6(a) %::
          f7(a) %::
          f8(a) %::
          f9(a) %::
          f10(a) %::
          f11(a) %::
          f12(a) %::
          f13(a) %::
          f14(a) %::
          f15(a) %::
          f16(a) %::
          f17(a) %::
          f18(a) %::
          f19(a) %::
          f20(a) %::
          f21(a) %::
          f22(a) %::
        DNil,
      self.filterOpt
    )
}
