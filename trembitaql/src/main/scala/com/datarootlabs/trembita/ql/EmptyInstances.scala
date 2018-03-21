package com.datarootlabs.trembita.ql


import shapeless._

import scala.reflect.macros.blackbox


trait EmptyInstances {
  implicit def numericEmpty[N](implicit N: Numeric[N]): Empty[N] = new Empty[N] {
    def empty: N = N.zero
  }
  implicit object EmptyString extends Empty[String] {
    override def empty: String = ""
  }
  implicit object EmptyBoolean extends Empty[Boolean] {
    def empty: Boolean = false
  }
  implicit object ShortEmpty extends Empty[Short] {
    def empty: Short = 0
  }
  implicit object ByteEmpty extends Empty[Byte] {
    def empty: Byte = 0
  }
  implicit object IntEmpty extends Empty[Int] {
    def empty: Int = 0
  }
  implicit object LongEmpty extends Empty[Long] {
    def empty: Long = 0
  }
  implicit object FloatEmpty extends Empty[Float] {
    def empty: Float = 0
  }
  implicit object DoubleEmpty extends Empty[Double] {
    def empty: Double = 0
  }
  implicit object BigIntEmpty extends Empty[BigInt] {
    def empty: BigInt = 0
  }

  implicit object BigDecimalEmpty extends Empty[BigDecimal] {
    def empty: BigDecimal = 0
  }
  def hListEmptyImpl[H <: HList : c.WeakTypeTag](c: blackbox.Context): c.Expr[Empty[H]] = {
    import c.universe._

    val hnil = typeOf[HNil].dealias
    val K = weakTypeOf[H].dealias

    val expr = K match {
      case `hnil` ⇒ q"HNilEmpty"
      case _      ⇒ K.typeArgs match {
        case List(head, tail) ⇒ q"consEmpty[$head, $tail]"
        case other            ⇒ throw new IllegalArgumentException(s"Unexpected $K in Empty[HList] macro")
      }
    }
    c.Expr[Empty[H]](expr)

  }

  object HNilEmpty extends Empty[HNil] {
    override def empty: HNil = HNil
  }
  def consEmpty[H: Empty, T <: HList : Empty]: Empty[H :: T] = new Empty[H :: T] {
    private val H = Empty[H]
    private val T = Empty[T]
    override def empty: H :: T = H.empty :: T.empty
  }
}
