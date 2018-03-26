package com.datarootlabs.trembita.ql

import shapeless._


case class ##[A, U](value: A)
sealed trait GroupingCriteria extends Product with Serializable {
  type Key
  type Tail <: GroupingCriteria
  def key: Key
  def tail: Tail
}

object GroupingCriteria {
  sealed trait GNil extends GroupingCriteria {
    type Key = GNil
    type Tail = GNil
    def key: GNil = GNil
    def tail: GNil = GNil
    def &::[GH <: ##[_, _]](head: GH): GH &:: GNil = GroupingCriteria.&::(head, this)
  }
  case object GNil extends GNil

  case class MultiKey[K]()
  implicit def multiKey2K[K](multiKey: MultiKey[K]): K = ???

  case class MultipleKeys[K <: GroupingCriteria](keys: Seq[K]) extends GroupingCriteria {
    type Key = K#Key
    type Tail = MultipleKeys[K#Tail]
    val key: Key = MultiKey[K#Key]()
    def tail: Tail = MultipleKeys(keys.map(_.tail))
  }

  case class &::[GH <: ##[_, _], GT <: GroupingCriteria](first: GH, rest: GT) extends GroupingCriteria {
    type Key = GH
    type Tail = GT
    val key : GH = first
    val tail: GT = rest
    override def toString: String = s"$first &:: $rest"
  }
}

sealed trait Aggregation extends Product with Serializable

object Aggregation {
  sealed trait AgNil extends Aggregation {
    def %::[AgH <: ##[_, _]](head: AgH): AgH %:: AgNil = Aggregation.%::(head, this)
  }
  case object AgNil extends AgNil
  case class %::[AgH <: ##[_, _], AgT <: Aggregation](agHead: AgH, agTail: AgT) extends Aggregation {
    override def toString: String = s"$agHead %:: $agTail"

  }
}